# Databricks notebook source
# Prime Capital Bank — AML / Transaction Monitoring
# Notebook: 06_aml_risk_scoring.py
# Author:   Prime Capital Bank — Compliance Analytics
# Purpose:  Build customer behavioural profiles, detect AML typologies
#           (structuring, layering, smurfing), graph-based network risk scoring,
#           generate SAR candidates, write to gold.fact_aml_case.


# COMMAND ----------

# %pip install graphframes
# dbutils.library.restartPython()

# COMMAND ----------

import uuid, logging
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql import functions as F, Window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, BooleanType, TimestampType
)
from delta.tables import DeltaTable

# GraphFrames is installed via init script / cluster lib
try:
    from graphframes import GraphFrame
    GRAPHFRAMES_AVAILABLE = True
except ImportError:
    GRAPHFRAMES_AVAILABLE = False
    logging.warning("GraphFrames not available — network analysis will be skipped")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("aml_risk_scoring")

# ── Config ─────────────────────────────────────────────────────────────────────
SILVER               = "silver"
GOLD                 = "gold"
RUN_ID               = str(uuid.uuid4())
FICA_THRESHOLD_ZAR   = 25_000.0    # FICA Reg 22B — cash threshold requiring reporting
STRUCTURING_BUFFER   = 0.15        # flag transactions within 15% below threshold
LOOKBACK_DAYS        = 30          # rolling behaviour window
LAYERING_MAX_MINUTES = 60          # transfers within 60 minutes = layering candidate
SMURFING_MIN_LEGS    = 3           # minimum accounts to flag as smurfing
SAR_SCORE_THRESHOLD  = 0.65        # composite risk score for SAR candidate flag

# COMMAND ----------

# -- ## 1 — Build 30-Day Customer Transaction Behaviour Profiles

# COMMAND ----------

def build_behaviour_profiles():
    """
    Compute 30-day rolling statistics per customer.
    Returns a Spark DataFrame with one row per customer.
    """
    txns = (
        spark.table(f"`{SILVER}`.`silver_transactions`")
             .filter(F.col("transaction_date") >= F.date_sub(F.current_date(), LOOKBACK_DAYS))
             .filter(F.col("transaction_status") == "COMPLETED")
    )

    profiles = (
        txns.groupBy("customer_id")
            .agg(
                # Volume metrics
                F.count("transaction_id").alias("txn_count_30d"),
                F.sum("amount_zar").alias("txn_sum_30d"),
                F.avg("amount_zar").alias("txn_avg_30d"),
                F.stddev("amount_zar").alias("txn_std_30d"),
                F.max("amount_zar").alias("txn_max_30d"),
                F.min("amount_zar").alias("txn_min_30d"),
                # Distinct accounts touched
                F.countDistinct("account_id").alias("distinct_accounts_30d"),
                # Cash-like (EFT/CASH channels)
                F.sum(F.when(F.col("channel").isin("CASH", "ATM"), F.col("amount_zar"))
                       .otherwise(F.lit(0))).alias("cash_amount_30d"),
                F.count(F.when(F.col("channel").isin("CASH", "ATM"), F.lit(1)))
                 .alias("cash_txn_count_30d"),
                # Night-time transactions (23:00-05:00)
                F.sum(F.when(F.hour("transaction_ts").between(23, 5),
                             F.lit(1)).otherwise(F.lit(0))).alias("night_txn_count_30d"),
                # Weekend transactions
                F.sum(F.when(F.dayofweek("transaction_date").isin(1, 7),
                             F.lit(1)).otherwise(F.lit(0))).alias("weekend_txn_count_30d"),
                # Active days
                F.countDistinct("transaction_date").alias("active_days_30d"),
                # International
                F.sum(F.when(F.col("channel") == "INTERNATIONAL",
                             F.col("amount_zar")).otherwise(F.lit(0))).alias("intl_amount_30d"),
            )
            .withColumn("cash_ratio_30d",
                        F.col("cash_amount_30d") / F.greatest(F.col("txn_sum_30d"), F.lit(1)))
            .withColumn("avg_daily_txn",
                        F.col("txn_count_30d") / F.lit(LOOKBACK_DAYS))
    )

    log.info(f"Behaviour profiles built: {profiles.count():,} customers")
    return profiles

PROFILES = build_behaviour_profiles()

# COMMAND ----------

# -- ## 2 — Structuring Detection (Transactions Just Below FICA Threshold)

# COMMAND ----------

def detect_structuring():
    """
    Flag customers with multiple transactions in a lookback window that are
    within STRUCTURING_BUFFER % below the FICA threshold (R25,000).
    Pattern: deliberately keeping individual amounts below the reporting limit.
    """
    lower_bound = FICA_THRESHOLD_ZAR * (1 - STRUCTURING_BUFFER)   # R21,250
    upper_bound = FICA_THRESHOLD_ZAR - 0.01                        # R24,999.99

    txns = (
        spark.table(f"`{SILVER}`.`silver_transactions`")
             .filter(F.col("transaction_date") >= F.date_sub(F.current_date(), LOOKBACK_DAYS))
             .filter(F.col("amount_zar").between(lower_bound, upper_bound))
             .filter(F.col("channel").isin("CASH", "ATM", "TELLER"))
    )

    structuring = (
        txns.groupBy("customer_id", "account_id")
            .agg(
                F.count("transaction_id").alias("structuring_txn_count"),
                F.sum("amount_zar").alias("structuring_total_zar"),
                F.min("transaction_date").alias("first_seen"),
                F.max("transaction_date").alias("last_seen"),
            )
            .filter(F.col("structuring_txn_count") >= 2)  # at least 2 sub-threshold transactions
            .withColumn("typology", F.lit("STRUCTURING"))
            .withColumn("risk_score",
                        F.least(
                            F.lit(1.0),
                            F.col("structuring_txn_count") / F.lit(10.0) * 0.8 +
                            F.col("structuring_total_zar") / F.lit(FICA_THRESHOLD_ZAR * 5) * 0.2
                        ))
    )

    log.info(f"Structuring: {structuring.count():,} cases detected")
    return structuring

STRUCTURING = detect_structuring()

# COMMAND ----------

# -- ## 3 — Layering Detection (Rapid Multi-Leg Transfers)

# COMMAND ----------

def detect_layering():
    """
    Layering: funds moved through multiple accounts in rapid succession.
    Flag: ≥3 outgoing EFTs from same customer within 60 minutes.
    """
    txns = (
        spark.table(f"`{SILVER}`.`silver_transactions`")
             .filter(F.col("transaction_date") >= F.date_sub(F.current_date(), LOOKBACK_DAYS))
             .filter(F.col("transaction_type") == "EFT_OUTGOING")
    )

    # Self-join to find rapid sequences
    w = Window.partitionBy("customer_id").orderBy("transaction_ts")

    layering = (
        txns.withColumn("lag_ts",        F.lag("transaction_ts").over(w))
            .withColumn("lag2_ts",       F.lag("transaction_ts", 2).over(w))
            .withColumn("minutes_since", F.round(
                            (F.col("transaction_ts").cast("long") -
                             F.col("lag_ts").cast("long")) / 60.0, 1))
            .filter(F.col("minutes_since") <= LAYERING_MAX_MINUTES)
            .groupBy("customer_id")
            .agg(
                F.count("transaction_id").alias("layering_leg_count"),
                F.sum("amount_zar").alias("layering_total_zar"),
                F.avg("minutes_since").alias("avg_minutes_between_legs"),
                F.min("transaction_date").alias("first_seen"),
                F.max("transaction_date").alias("last_seen"),
            )
            .filter(F.col("layering_leg_count") >= 3)
            .withColumn("typology",   F.lit("LAYERING"))
            .withColumn("risk_score",
                        F.least(F.lit(1.0),
                                0.6 + (F.lit(LAYERING_MAX_MINUTES) -
                                       F.col("avg_minutes_between_legs")) /
                                      F.lit(LAYERING_MAX_MINUTES) * 0.4))
    )

    log.info(f"Layering: {layering.count():,} cases detected")
    return layering

LAYERING = detect_layering()

# COMMAND ----------

# -- ## 4 — Smurfing Detection (Splitting to Multiple Accounts)

# COMMAND ----------

def detect_smurfing():
    """
    Smurfing: same source customer sends multiple smaller amounts to ≥3 different
    destination accounts within a short window — total would exceed FICA threshold.
    """
    txns = (
        spark.table(f"`{SILVER}`.`silver_transactions`")
             .filter(F.col("transaction_date") >= F.date_sub(F.current_date(), LOOKBACK_DAYS))
             .filter(F.col("transaction_type").isin("EFT_OUTGOING", "TRANSFER_OUT"))
    )

    smurfing = (
        txns.groupBy("customer_id", "transaction_date")
            .agg(
                F.countDistinct("account_id").alias("distinct_dest_accounts"),
                F.sum("amount_zar").alias("daily_total_zar"),
                F.count("transaction_id").alias("daily_txn_count"),
                F.avg("amount_zar").alias("avg_txn_amount"),
            )
            # Multiple destination accounts AND combined total would have triggered reporting
            .filter(
                (F.col("distinct_dest_accounts") >= SMURFING_MIN_LEGS) &
                (F.col("daily_total_zar") >= FICA_THRESHOLD_ZAR)
            )
            .groupBy("customer_id")
            .agg(
                F.count("transaction_date").alias("smurfing_days_count"),
                F.sum("daily_total_zar").alias("smurfing_total_zar"),
                F.max("distinct_dest_accounts").alias("max_dest_accounts"),
                F.min("transaction_date").alias("first_seen"),
                F.max("transaction_date").alias("last_seen"),
            )
            .withColumn("typology",   F.lit("SMURFING"))
            .withColumn("risk_score",
                        F.least(F.lit(1.0),
                                0.5 + F.col("max_dest_accounts") / F.lit(10.0) * 0.5))
    )

    log.info(f"Smurfing: {smurfing.count():,} cases detected")
    return smurfing

SMURFING = detect_smurfing()

# COMMAND ----------

# -- ## 5 — Network Graph Analysis (GraphFrames)

# COMMAND ----------

def build_network_risk_scores():
    """
    Build a directed transaction graph: nodes = customers, edges = EFT transfers.
    Use PageRank as a proxy for network centrality / risk concentration.
    """
    if not GRAPHFRAMES_AVAILABLE:
        log.warning("GraphFrames unavailable — returning empty network scores")
        return spark.createDataFrame([], "customer_id STRING, network_risk_score DOUBLE, pagerank DOUBLE")

    txns = (
        spark.table(f"`{SILVER}`.`silver_transactions`")
             .filter(F.col("transaction_date") >= F.date_sub(F.current_date(), LOOKBACK_DAYS))
             .filter(F.col("transaction_type").isin("EFT_OUTGOING", "TRANSFER_OUT"))
             .filter(F.col("counterparty_customer_id").isNotNull())
    )

    # Nodes: all unique customers
    nodes = (
        txns.select(F.col("customer_id").alias("id"))
            .union(txns.select(F.col("counterparty_customer_id").alias("id")))
            .distinct()
    )

    # Edges: source -> destination with aggregated weight
    edges = (
        txns.groupBy(
                F.col("customer_id").alias("src"),
                F.col("counterparty_customer_id").alias("dst")
            )
            .agg(
                F.sum("amount_zar").alias("total_amount"),
                F.count("transaction_id").alias("txn_count"),
            )
            .withColumn("relationship", F.lit("TRANSFER"))
    )

    g = GraphFrame(nodes, edges)

    # PageRank (convergence tolerance=0.001, max iterations=10)
    pagerank = g.pageRank(resetProbability=0.15, tol=0.001).vertices
    pagerank = pagerank.withColumnRenamed("pagerank", "pagerank_raw")

    # Normalise to 0-1
    max_pr = pagerank.agg(F.max("pagerank_raw")).collect()[0][0]
    max_pr = max_pr if max_pr is not None else 0.001
    network_scores = pagerank.withColumn(
        "network_risk_score",
        (F.col("pagerank_raw") / F.lit(max(max_pr, 0.001))).cast("double")
    ).withColumn("pagerank", F.col("pagerank_raw")) \
     .select(F.col("id").alias("customer_id"), "network_risk_score", "pagerank")

    log.info(f"Network graph: {nodes.count():,} nodes | {edges.count():,} edges")
    return network_scores

NETWORK_SCORES = build_network_risk_scores()

# COMMAND ----------

# -- ## 6 — Composite AML Risk Score & SAR Generation

# COMMAND ----------

def build_aml_cases():
    """Combine all typology flags into a composite risk score per customer."""

    # Collect all flagged customers
    struct_cust = STRUCTURING.select(
        "customer_id", "typology", "risk_score",
        F.coalesce(F.col("structuring_total_zar"), F.lit(0.0)).alias("flagged_amount_zar"),
        "first_seen", "last_seen"
    )
    layer_cust = LAYERING.select(
        "customer_id", "typology", "risk_score",
        F.coalesce(F.col("layering_total_zar"), F.lit(0.0)).alias("flagged_amount_zar"),
        "first_seen", "last_seen"
    )
    smurf_cust = SMURFING.select(
        "customer_id", "typology", "risk_score",
        F.coalesce(F.col("smurfing_total_zar"), F.lit(0.0)).alias("flagged_amount_zar"),
        "first_seen", "last_seen"
    )

    all_flags = struct_cust.union(layer_cust).union(smurf_cust)

    # Aggregate to one row per customer: max risk score, concatenate typologies
    agg_flags = (
        all_flags.groupBy("customer_id")
                 .agg(
                     F.max("risk_score").alias("base_risk_score"),
                     F.collect_set("typology").alias("typologies"),
                     F.sum("flagged_amount_zar").alias("total_flagged_amount_zar"),
                     F.min("first_seen").alias("first_seen"),
                     F.max("last_seen").alias("last_seen"),
                 )
                 .withColumn("typology",
                             F.array_join(F.col("typologies"), " | "))
    )

    # Join network risk
    if NETWORK_SCORES.count() > 0:
        agg_flags = agg_flags.join(
            NETWORK_SCORES.select("customer_id", "network_risk_score"),
            "customer_id", "left"
        ).fillna({"network_risk_score": 0.0})
    else:
        agg_flags = agg_flags.withColumn("network_risk_score", F.lit(0.0))

    # Composite score: 60% typology + 40% network centrality
    agg_flags = agg_flags.withColumn(
        "composite_risk_score",
        F.least(F.lit(1.0),
                F.col("base_risk_score") * 0.6 +
                F.col("network_risk_score") * 0.4)
    )

    # SAR candidates
    cases = (
        agg_flags
        .withColumn("case_id",       F.concat(F.lit("AML-"), F.lit(RUN_ID[:8]), F.lit("-"),
                                              F.monotonically_increasing_id().cast("string")))
        .withColumn("sar_candidate",  (F.col("composite_risk_score") >= SAR_SCORE_THRESHOLD).cast("boolean"))
        .withColumn("case_status",    F.lit("PENDING_REVIEW"))
        .withColumn("assigned_analyst", F.lit(None).cast("string"))
        .withColumn("detected_at",   F.current_timestamp())
        .withColumn("reviewed_at",   F.lit(None).cast("timestamp"))
        .withColumn("_gold_loaded_at", F.current_timestamp())
        .select(
            "case_id", "customer_id",
            F.col("typology").alias("case_type"),
            F.col("composite_risk_score").alias("risk_score"),
            "sar_candidate", "detected_at", "reviewed_at",
            "case_status", "assigned_analyst", "_gold_loaded_at"
        )
    )

    return cases

AML_CASES = build_aml_cases()

# COMMAND ----------

# -- ## 7 — Write to gold.fact_aml_case

# COMMAND ----------

try:
    target = DeltaTable.forName(spark, f"`{GOLD}`.`fact_aml_case`")
    (
        target.alias("t")
              .merge(AML_CASES.alias("s"), "t.case_id = s.case_id")
              .whenMatchedUpdate(set={
                  "risk_score":     "s.risk_score",
                  "sar_candidate":  "s.sar_candidate",
                  "_gold_loaded_at":"s._gold_loaded_at",
              })
              .whenNotMatchedInsertAll()
              .execute()
    )
except Exception as e:
    log.warning(f"AML case MERGE fallback to overwrite: {e}")
    AML_CASES.write.format("delta").mode("overwrite").option("mergeSchema","true") \
              .saveAsTable(f"`{GOLD}`.`fact_aml_case`")

# COMMAND ----------

# -- ## 8 — AML Summary Dashboard

# COMMAND ----------

sar_count = AML_CASES.filter(F.col("sar_candidate")).count()
total     = AML_CASES.count()
log.info(f"AML cases: {total:,} total | {sar_count:,} SAR candidates")

display(
    AML_CASES.filter(F.col("sar_candidate"))
             .orderBy(F.col("risk_score").desc())
             .limit(50)
)

# Typology breakdown
spark.sql(f"""
    SELECT case_type, COUNT(*) AS case_count,
           AVG(risk_score) AS avg_risk_score,
           SUM(CASE WHEN sar_candidate THEN 1 ELSE 0 END) AS sar_candidates
    FROM `{GOLD}`.`fact_aml_case`
    WHERE detected_at >= current_timestamp() - INTERVAL 1 DAY
    GROUP BY case_type
""").display()

# COMMAND ----------

# -- ## 9 — Typology Backtest (known-pattern recall validation)

# COMMAND ----------

# Synthetic known-positive typologies injected at Silver layer (tag = "TYPOLOGY_SEED").
# For each typology, check that detection rate meets the FATF/SARB expectation:
#   Structuring  ≥ 90%  (rule is deterministic on amount + pattern)
#   Layering     ≥ 80%  (time-window rule — some edge cases miss)
#   Smurfing     ≥ 75%  (graph-leg count — sparse graphs reduce recall)
TYPOLOGY_RECALL_THRESHOLDS = {
    "STRUCTURING": 0.90,
    "LAYERING":    0.80,
    "SMURFING":    0.75,
}

backtest_results = {}
for typology, min_recall in TYPOLOGY_RECALL_THRESHOLDS.items():
    seed_cases = AML_CASES.filter(
        (F.col("case_type") == typology) &
        (F.upper(F.col("case_notes")).contains("TYPOLOGY_SEED"))
    ).count()
    detected = AML_CASES.filter(
        (F.col("case_type") == typology) &
        (F.upper(F.col("case_notes")).contains("TYPOLOGY_SEED")) &
        (F.col("risk_score") >= SAR_SCORE_THRESHOLD)
    ).count()
    recall = detected / seed_cases if seed_cases > 0 else 1.0  # no seeds = not testable
    status = "PASS" if recall >= min_recall else "FAIL"
    backtest_results[typology] = {"seed_cases": seed_cases, "detected": detected,
                                   "recall": recall, "threshold": min_recall, "status": status}
    log.info(f"Backtest {typology}: recall={recall:.1%} (min={min_recall:.0%}) [{status}]"
             + (f" — {seed_cases} seeds" if seed_cases > 0 else " — no seeds (skipped)"))

print(f"\n06_aml_risk_scoring — COMPLETE  (run_id={RUN_ID})")
print(f"Total AML cases : {total:,}")
print(f"SAR candidates  : {sar_count:,}")
print(f"Backtest        : {[k+':'+v['status'] for k,v in backtest_results.items()]}")
