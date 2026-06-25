# Databricks notebook source
# Prime Capital Bank — Silver Transformation Layer
# Notebook: 02_silver_transformation.py
# Author:   Prime Capital Bank — Data Engineering
# Purpose:  Read bronze Delta tables, apply cleansing, deduplication,
#           PII masking, address standardisation, currency conversion,
#           and write conformed records to silver schema.


# COMMAND ----------

import uuid, logging, hashlib
from datetime import datetime
from pyspark.sql import functions as F, Window
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    TimestampType, DoubleType, IntegerType, DateType, BooleanType
)
from delta.tables import DeltaTable

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("silver_transformation")

# ── Configuration ─────────────────────────────────────────────────────────────
BRONZE        = "bronze"
SILVER        = "silver"
ADLS_ACCOUNT  = "primecapitaladls"
RUN_ID        = str(uuid.uuid4())
# Detect environment from Databricks tags or widget
ENV = dbutils.widgets.get("env") if "env" in [w.name for w in dbutils.widgets.getAll()] else "nonprod"
IS_PROD       = ENV.lower() == "prod"

FOREX_TABLE   = f"`{BRONZE}`.`bronze_forex_rates`"
DQ_METRICS_TABLE = f"`{SILVER}`.`dq_metrics`"

# COMMAND ----------

# -- ## 0 — Create Silver Control Tables

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS `{SILVER}`.`dq_metrics` (
        run_id          STRING,
        table_name      STRING,
        metric_name     STRING,
        metric_value    DOUBLE,
        recorded_at     TIMESTAMP
    )
    USING DELTA
    TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
""")

# COMMAND ----------

# -- ## 1 — PII Masking Utilities

# COMMAND ----------

def mask_pii(df, cols: list):
    """
    In non-prod: SHA-256 hash the listed columns.
    In prod: return df unchanged (masking not applied to prod silver — handled at gold/mart layer).
    """
    if IS_PROD:
        return df
    for col in cols:
        if col in df.columns:
            df = df.withColumn(
                col,
                F.when(
                    F.col(col).isNotNull(),
                    F.sha2(F.col(col).cast("string"), 256)
                ).otherwise(F.lit(None))
            )
    return df


def nullify_invalid_sa_id(df, id_col: str):
    """Zero out statistically invalid 13-digit SA ID numbers."""
    if id_col not in df.columns:
        return df
    return df.withColumn(
        id_col,
        F.when(F.length(F.col(id_col)) == 13, F.col(id_col)).otherwise(F.lit(None))
    )

# COMMAND ----------

# -- ## 2 — Currency Conversion Utility

# COMMAND ----------

def get_latest_forex_rates() -> dict:
    """Load latest forex rates -> ZAR from bronze_forex_rates."""
    try:
        rates_df = (
            spark.table(FOREX_TABLE)
                 .filter(F.col("currency_pair").endswith("ZAR"))
                 .groupBy("currency_pair")
                 .agg(F.last("rate", ignorenulls=True).alias("rate"))
                 .collect()
        )
        return {row["currency_pair"].replace("ZAR", "").strip("/"): float(row["rate"])
                for row in rates_df}
    except Exception as e:
        log.warning(f"Forex rates unavailable, using fallback: {e}")
        # Approximate fallback rates (to ZAR)
        return {"USD": 18.5, "EUR": 20.1, "GBP": 23.4, "USD/": 18.5}


def add_zar_amount(df, amount_col: str, currency_col: str, zar_col: str = None) -> object:
    """Multiply amount by the forex rate to produce a ZAR equivalent column."""
    if amount_col not in df.columns:
        return df
    if zar_col is None:
        zar_col = f"{amount_col}_zar"
    rates = get_latest_forex_rates()
    # Build CASE expression
    case_expr = F.when(F.col(currency_col).isNull() | (F.col(currency_col) == "ZAR"),
                       F.col(amount_col))
    for ccy, rate in rates.items():
        case_expr = case_expr.when(F.col(currency_col) == ccy,
                                   F.col(amount_col) * F.lit(rate))
    case_expr = case_expr.otherwise(F.col(amount_col))  # unknown currency — pass through
    return df.withColumn(zar_col, case_expr.cast("double"))

# COMMAND ----------

# -- ## 3 — Deduplication Helper

# COMMAND ----------

def deduplicate(df, pk_cols: list, order_col: str = "_ingestion_timestamp"):
    """
    Keep the latest record per natural key using ROW_NUMBER() OVER (PARTITION BY pk ORDER BY order_col DESC).
    """
    if not pk_cols or not all(p in df.columns for p in pk_cols):
        log.warning(f"Skipping dedup — PK columns missing: {pk_cols}")
        return df
    window = Window.partitionBy(*pk_cols).orderBy(F.col(order_col).desc())
    return (
        df.withColumn("_rn", F.row_number().over(window))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )

# COMMAND ----------

# -- ## 4 — DQ Metric Publisher

# COMMAND ----------

def publish_dq_metrics(table_name: str, metrics: dict):
    rows = [(RUN_ID, table_name, k, float(v), datetime.utcnow()) for k, v in metrics.items()]
    schema = "run_id STRING, table_name STRING, metric_name STRING, metric_value DOUBLE, recorded_at TIMESTAMP"
    mdf = spark.createDataFrame(rows, schema=schema)
    mdf.write.format("delta").mode("append").saveAsTable(DQ_METRICS_TABLE)

# COMMAND ----------

# -- ## 5 — Address Standardisation

# COMMAND ----------

def standardise_address(df, street_col="street_address", suburb_col="suburb",
                         city_col="city", province_col="province", postal_col="postal_code"):
    """Basic address standardisation: trim whitespace, upper-case province, normalise postal code."""
    cols_present = df.columns
    if street_col in cols_present:
        df = df.withColumn(street_col, F.initcap(F.trim(F.col(street_col))))
    if suburb_col in cols_present:
        df = df.withColumn(suburb_col, F.initcap(F.trim(F.col(suburb_col))))
    if city_col in cols_present:
        df = df.withColumn(city_col, F.initcap(F.trim(F.col(city_col))))
    if province_col in cols_present:
        df = df.withColumn(province_col, F.upper(F.trim(F.col(province_col))))
    if postal_col in cols_present:
        # South African postal codes are 4 digits — zero-pad
        df = df.withColumn(postal_col, F.lpad(F.regexp_replace(F.col(postal_col), r"\D", ""), 4, "0"))
    return df

# COMMAND ----------

# -- ## 6 — Transform: Customers

# COMMAND ----------

def transform_customers():
    df = (
        spark.table(f"`{BRONZE}`.`bronze_customers`")
             .filter(F.col("customer_id").isNotNull())
    )

    df = deduplicate(df, ["customer_id"])
    df = nullify_invalid_sa_id(df, "id_number")
    df = mask_pii(df, ["email_address", "mobile_number", "id_number", "tax_number"])
    df = standardise_address(df)

    df = (
        df.withColumn("date_of_birth",   F.col("date_of_birth").cast("date"))
          .withColumn("onboarding_date", F.col("onboarding_date").cast("date"))
          .withColumn("risk_segment",    F.upper(F.trim(F.col("risk_segment"))))
          .withColumn("customer_status", F.upper(F.trim(F.col("customer_status"))))
          .withColumn("age_years",
                      F.floor(F.datediff(F.current_date(), F.col("date_of_birth")) / 365.25))
          .withColumn("_silver_loaded_at", F.current_timestamp())
          .withColumn("_silver_run_id",    F.lit(RUN_ID))
    )

    total   = df.count()
    nulls   = df.filter(F.col("customer_id").isNull()).count()
    publish_dq_metrics("silver_customers", {"total_rows": total, "null_pks": nulls})

    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
      .saveAsTable(f"`{SILVER}`.`silver_customers`")
    log.info(f"silver_customers — {total:,} rows written")

transform_customers()

# COMMAND ----------

# -- ## 7 — Transform: Accounts

# COMMAND ----------

def transform_accounts():
    df = (
        spark.table(f"`{BRONZE}`.`bronze_accounts`")
             .filter(F.col("account_id").isNotNull())
    )
    df = deduplicate(df, ["account_id"])
    df = (
        df.withColumn("open_date",        F.col("open_date").cast("date"))
          .withColumn("close_date",       F.col("close_date").cast("date"))
          .withColumn("current_balance",  F.col("current_balance").cast("double"))
          .withColumn("available_balance",F.col("available_balance").cast("double"))
          .withColumn("account_status",   F.upper(F.trim(F.col("account_status"))))
          .withColumn("account_type",     F.upper(F.trim(F.col("account_type"))))
          .withColumn("days_open",
                      F.datediff(F.coalesce(F.col("close_date"), F.current_date()), F.col("open_date")))
          .withColumn("_silver_loaded_at", F.current_timestamp())
          .withColumn("_silver_run_id",    F.lit(RUN_ID))
    )
    total = df.count()
    publish_dq_metrics("silver_accounts", {"total_rows": total})
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
      .saveAsTable(f"`{SILVER}`.`silver_accounts`")
    log.info(f"silver_accounts — {total:,} rows written")

transform_accounts()

# COMMAND ----------

# -- ## 8 — Transform: Transactions

# COMMAND ----------

def transform_transactions():
    df = (
        spark.table(f"`{BRONZE}`.`bronze_transactions`")
             .filter(F.col("transaction_id").isNotNull())
    )
    df = deduplicate(df, ["transaction_id"])
    df = add_zar_amount(df, "amount", "currency_code", "amount_zar")
    df = (
        df.withColumn("transaction_date",    F.col("transaction_date").cast("date"))
          .withColumn("transaction_ts",      F.col("transaction_ts").cast("timestamp"))
          .withColumn("amount",              F.col("amount").cast("double"))
          .withColumn("transaction_type",    F.upper(F.trim(F.col("transaction_type"))))
          .withColumn("transaction_status",  F.upper(F.trim(F.col("transaction_status"))))
          .withColumn("channel",             F.upper(F.trim(F.col("channel"))))
          # Flag same-day reversals
          .withColumn("is_reversal",
                      F.col("transaction_type").isin("REVERSAL", "CHARGEBACK").cast("boolean"))
          .withColumn("_silver_loaded_at", F.current_timestamp())
          .withColumn("_silver_run_id",    F.lit(RUN_ID))
    )
    total = df.count()
    neg   = df.filter(F.col("amount") < 0).count()
    publish_dq_metrics("silver_transactions",
                       {"total_rows": total, "negative_amount_rows": neg})
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
            .partitionBy("transaction_date") \
      .saveAsTable(f"`{SILVER}`.`silver_transactions`")
    log.info(f"silver_transactions — {total:,} rows written")

transform_transactions()

# COMMAND ----------

# -- ## 9 — Transform: Card Transactions

# COMMAND ----------

def transform_card_transactions():
    df = (
        spark.table(f"`{BRONZE}`.`bronze_card_transactions`")
             .filter(F.col("card_txn_id").isNotNull())
    )
    df = deduplicate(df, ["card_txn_id"])
    df = add_zar_amount(df, "txn_amount", "currency_code", "txn_amount_zar")
    df = mask_pii(df, ["card_number", "masked_pan"])
    df = (
        df.withColumn("txn_date",          F.col("txn_date").cast("date"))
          .withColumn("txn_ts",            F.col("txn_ts").cast("timestamp"))
          .withColumn("txn_amount",        F.col("txn_amount").cast("double"))
          .withColumn("mcc_code",          F.trim(F.col("mcc_code")))
          .withColumn("is_international",  (F.col("currency_code") != "ZAR").cast("boolean"))
          .withColumn("_silver_loaded_at", F.current_timestamp())
          .withColumn("_silver_run_id",    F.lit(RUN_ID))
    )
    total = df.count()
    publish_dq_metrics("silver_card_transactions", {"total_rows": total})
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
            .partitionBy("txn_date") \
      .saveAsTable(f"`{SILVER}`.`silver_card_transactions`")
    log.info(f"silver_card_transactions — {total:,} rows written")

transform_card_transactions()

# COMMAND ----------

# -- ## 10 — Transform: Loans

# COMMAND ----------

def transform_loans():
    df = (
        spark.table(f"`{BRONZE}`.`bronze_loans`")
             .filter(F.col("loan_id").isNotNull())
    )
    df = deduplicate(df, ["loan_id"])
    df = add_zar_amount(df, "original_amount", "currency_code", "original_amount_zar")
    df = add_zar_amount(df, "outstanding_balance", "currency_code", "outstanding_balance_zar")
    df = (
        df.withColumn("origination_date",   F.col("origination_date").cast("date"))
          .withColumn("maturity_date",       F.col("maturity_date").cast("date"))
          .withColumn("interest_rate",       F.col("interest_rate").cast("double"))
          .withColumn("original_amount",     F.col("original_amount").cast("double"))
          .withColumn("outstanding_balance", F.col("outstanding_balance").cast("double"))
          .withColumn("loan_status",         F.upper(F.trim(F.col("loan_status"))))
          .withColumn("loan_type",           F.upper(F.trim(F.col("loan_type"))))
          .withColumn("days_past_due",       F.col("days_past_due").cast("integer"))
          .withColumn("is_npl",
                      (F.col("days_past_due") >= 90).cast("boolean"))
          .withColumn("loan_term_months",
                      F.floor(F.months_between(F.col("maturity_date"),
                                               F.col("origination_date"))).cast("integer"))
          .withColumn("_silver_loaded_at", F.current_timestamp())
          .withColumn("_silver_run_id",    F.lit(RUN_ID))
    )
    total = df.count()
    npl   = df.filter(F.col("is_npl")).count()
    publish_dq_metrics("silver_loans",
                       {"total_rows": total, "npl_count": npl,
                        "npl_rate_pct": round(npl / max(total, 1) * 100, 2)})
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
      .saveAsTable(f"`{SILVER}`.`silver_loans`")
    log.info(f"silver_loans — {total:,} rows | NPL: {npl:,} ({npl/max(total,1):.1%})")

transform_loans()

# COMMAND ----------

# -- ## 11 — DQ Summary Report

# COMMAND ----------

dq_summary = spark.sql(f"""
    SELECT table_name, metric_name, metric_value
    FROM {DQ_METRICS_TABLE}
    WHERE run_id = '{RUN_ID}'
    ORDER BY table_name, metric_name
""")
display(dq_summary)
print(f"\n02_silver_transformation — COMPLETE  (run_id={RUN_ID})")
