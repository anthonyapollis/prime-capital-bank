# Databricks notebook source
# Prime Capital Bank — Fraud Detection
# Notebook: 05_fraud_detection.py
# Author:   Prime Capital Bank — Data Science
# Purpose:  Velocity feature engineering, Isolation Forest + GBM ensemble,
#           structured streaming real-time scorer, alert threshold calibration,
#           write fraud scores to silver_card_transactions and gold.fact_fraud_event.


# COMMAND ----------

# %pip install lightgbm scikit-learn mlflow shap
# dbutils.library.restartPython()

# COMMAND ----------

import uuid, logging, warnings
import numpy as np
import pandas as pd
import mlflow
import mlflow.sklearn
import mlflow.lightgbm
import lightgbm as lgb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import shap

from datetime import datetime, timedelta
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.metrics import roc_auc_score, precision_recall_curve, confusion_matrix
from sklearn.model_selection import train_test_split
from pyspark.sql import functions as F, Window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    TimestampType, BooleanType
)
from delta.tables import DeltaTable

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("fraud_detection")

# ── Config ─────────────────────────────────────────────────────────────────────
SILVER               = "silver"
GOLD                 = "gold"
RUN_ID               = str(uuid.uuid4())
MLFLOW_EXPERIMENT    = "/Prime Capital Bank/Fraud Detection"
FRAUD_MODEL_NAME     = "prime_capital_fraud_ensemble"
TARGET_FPR           = 0.001     # 0.1% false positive rate
ADLS_ACCOUNT         = "primecapitaladls"

mlflow.set_experiment(MLFLOW_EXPERIMENT)

# ── MCC risk tiers (South African merchant categories) ───────────────────────
HIGH_RISK_MCC = {
    "7995": "GAMBLING", "6051": "CRYPTO_EXCHANGE", "5912": "PHARMACY",
    "7273": "ESCORT",   "5999": "MISC_RETAIL",     "4829": "WIRE_TRANSFER",
    "6099": "MONEY_SERVICES", "5933": "PAWN_SHOPS",
}
MEDIUM_RISK_MCC = {
    "5411": "GROCERY", "5812": "RESTAURANTS", "5541": "GAS_STATIONS",
    "7011": "HOTELS",  "4111": "TRANSPORT",
}

def mcc_risk_score(mcc: str) -> float:
    if mcc in HIGH_RISK_MCC:   return 1.0
    if mcc in MEDIUM_RISK_MCC: return 0.5
    return 0.0

# COMMAND ----------

# -- ## 1 — Velocity Feature Engineering

# COMMAND ----------

def build_velocity_features(df):
    """
    Compute velocity features over 1h, 24h, 7d windows using Spark window functions.
    Input: silver_card_transactions Spark DataFrame.
    """
    # Windows are defined per customer_id ordered by txn_ts (unix seconds)
    w1h  = Window.partitionBy("customer_id") \
                 .orderBy(F.col("txn_ts").cast("long")) \
                 .rangeBetween(-3600, 0)
    w24h = Window.partitionBy("customer_id") \
                 .orderBy(F.col("txn_ts").cast("long")) \
                 .rangeBetween(-86400, 0)
    w7d  = Window.partitionBy("customer_id") \
                 .orderBy(F.col("txn_ts").cast("long")) \
                 .rangeBetween(-604800, 0)

    df = (
        df
        # Velocity counts
        .withColumn("txn_count_1h",   F.count("card_txn_id").over(w1h))
        .withColumn("txn_count_24h",  F.count("card_txn_id").over(w24h))
        .withColumn("txn_count_7d",   F.count("card_txn_id").over(w7d))
        # Amount sums
        .withColumn("amount_sum_1h",  F.sum("txn_amount_zar").over(w1h))
        .withColumn("amount_sum_24h", F.sum("txn_amount_zar").over(w24h))
        .withColumn("amount_sum_7d",  F.sum("txn_amount_zar").over(w7d))
        # Amount z-score (vs 7d mean/std)
        .withColumn("amount_mean_7d", F.avg("txn_amount_zar").over(w7d))
        .withColumn("amount_std_7d",  F.stddev("txn_amount_zar").over(w7d))
        .withColumn("amount_zscore",
                    F.when(F.col("amount_std_7d") > 0,
                           (F.col("txn_amount_zar") - F.col("amount_mean_7d")) /
                           F.col("amount_std_7d"))
                     .otherwise(F.lit(0.0)))
        # Distinct merchants last 24h
        .withColumn("distinct_merchants_24h",
                    F.approx_count_distinct("mcc_code").over(w24h))
        # MCC risk score
        .withColumn("mcc_risk_score",
                    F.udf(mcc_risk_score, DoubleType())("mcc_code"))
        # Geo-velocity proxy: flag if same card used in international + domestic in <1h
        .withColumn("is_international",
                    (F.col("currency_code") != "ZAR").cast("double"))
        .withColumn("intl_count_1h",
                    F.sum("is_international").over(w1h))
        .withColumn("geo_velocity_flag",
                    (F.col("intl_count_1h") > 0).cast("double"))
        # Time-based features
        .withColumn("hour_of_day",    F.hour("txn_ts"))
        .withColumn("is_night",       F.when(F.col("hour_of_day").between(23, 5),
                                             F.lit(1)).otherwise(F.lit(0)))
        .withColumn("is_weekend",     F.dayofweek("txn_date").isin(1, 7).cast("double"))
        # Round-number flag (common in fraud)
        .withColumn("is_round_amount",
                    ((F.col("txn_amount_zar") % 100) == 0).cast("double"))
    )

    return df

# COMMAND ----------

# -- ## 2 — Load and Prepare Training Data

# COMMAND ----------

def load_training_data() -> pd.DataFrame:
    txns = spark.table(f"`{SILVER}`.`silver_card_transactions`")

    # Use 12 months of history for training
    txns = txns.filter(
        F.col("txn_date") >= F.add_months(F.current_date(), -12)
    )

    txns = build_velocity_features(txns)

    # If fraud_label exists use it; otherwise simulate for demo
    if "fraud_label" not in txns.columns:
        txns = txns.withColumn(
            "fraud_label",
            F.when(
                (F.col("amount_zscore") > 3.0) |
                (F.col("txn_count_1h") > 10)   |
                (F.col("mcc_risk_score") == 1.0),
                F.lit(1)
            ).otherwise(F.lit(0))
        )

    pdf = txns.toPandas()
    log.info(f"Training data: {len(pdf):,} rows | Fraud rate: {pdf['fraud_label'].mean():.3%}")
    return pdf

TRAIN_PDF = load_training_data()

# COMMAND ----------

# -- ## 3 — Feature Selection

# COMMAND ----------

FRAUD_FEATURES = [
    "txn_count_1h", "txn_count_24h", "txn_count_7d",
    "amount_sum_1h", "amount_sum_24h", "amount_sum_7d",
    "amount_zscore", "distinct_merchants_24h", "mcc_risk_score",
    "geo_velocity_flag", "hour_of_day", "is_night", "is_weekend",
    "is_round_amount", "txn_amount_zar", "is_international",
    "intl_count_1h",
]
AVAIL_FEATURES = [f for f in FRAUD_FEATURES if f in TRAIN_PDF.columns]

X = TRAIN_PDF[AVAIL_FEATURES].fillna(0)
y = TRAIN_PDF["fraud_label"].astype(int)

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

# COMMAND ----------

# -- ## 4 — Train Isolation Forest (Anomaly Detector)

# COMMAND ----------

scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled  = scaler.transform(X_test)

iso_forest = IsolationForest(
    n_estimators=300,
    contamination=max(y_train.mean(), 0.01),
    random_state=42,
    n_jobs=-1,
)
iso_forest.fit(X_train_scaled)

iso_scores_test = -iso_forest.score_samples(X_test_scaled)   # higher = more anomalous
iso_scores_test = (iso_scores_test - iso_scores_test.min()) / \
                  (iso_scores_test.max() - iso_scores_test.min() + 1e-10)

iso_auc = roc_auc_score(y_test, iso_scores_test)
log.info(f"Isolation Forest AUC: {iso_auc:.4f}")

# COMMAND ----------

# -- ## 5 — Train LightGBM Fraud Classifier

# COMMAND ----------

def train_fraud_gbm(X_train, y_train, X_test, y_test):
    lgb_params = {
        "objective":         "binary",
        "metric":            ["auc", "binary_logloss"],
        "n_estimators":      1200,
        "learning_rate":     0.03,
        "max_depth":         7,
        "num_leaves":        127,
        "min_child_samples": 50,
        "subsample":         0.8,
        "colsample_bytree":  0.8,
        "scale_pos_weight":  max(1, (y_train == 0).sum() / max((y_train == 1).sum(), 1)),
        "n_jobs":            -1,
        "random_state":      42,
    }

    model = lgb.LGBMClassifier(**lgb_params)

    with mlflow.start_run(run_name="Fraud_GBM") as run:
        mlflow.log_params(lgb_params)
        model.fit(
            X_train, y_train,
            eval_set=[(X_test, y_test)],
            callbacks=[lgb.early_stopping(100, verbose=False)],
        )
        gbm_probs = model.predict_proba(X_test)[:, 1]
        gbm_auc   = roc_auc_score(y_test, gbm_probs)
        mlflow.log_metric("gbm_auc", gbm_auc)
        log.info(f"GBM AUC: {gbm_auc:.4f}")

        # Feature importance plot
        fi = pd.Series(model.feature_importances_, index=AVAIL_FEATURES).sort_values(ascending=False)
        fig, ax = plt.subplots(figsize=(10, 6))
        fi.head(15).plot(kind="barh", ax=ax, color="#00B4D8")
        ax.set_title("Fraud GBM — Top 15 Feature Importances")
        ax.invert_yaxis()
        plt.tight_layout()
        mlflow.log_figure(fig, "fraud_feature_importance.png")
        plt.close(fig)

        mlflow.lightgbm.log_model(
            model, artifact_path="fraud_gbm",
            registered_model_name=FRAUD_MODEL_NAME,
            input_example=X_test.head(5),
        )

    return model, gbm_probs, run.info.run_id

GBM_MODEL, GBM_PROBS, FRAUD_RUN_ID = train_fraud_gbm(X_train, y_train, X_test, y_test)

# COMMAND ----------

# -- ## 6 — Ensemble Score & Alert Threshold Calibration

# COMMAND ----------

# Ensemble: 40% Isolation Forest + 60% GBM
iso_scores_for_ensemble = -GBM_MODEL.booster_.predict(
    X_test[AVAIL_FEATURES].values, raw_score=False
)  # reuse same X_test
ENSEMBLE_SCORES = 0.4 * iso_scores_test + 0.6 * GBM_PROBS

# Calibrate threshold to target 0.1% FPR
def calibrate_threshold(y_true, y_score, target_fpr: float = 0.001) -> float:
    precisions, recalls, thresholds = precision_recall_curve(y_true, y_score)
    fps = []
    tns = []
    for t in thresholds:
        preds = (y_score >= t).astype(int)
        cm    = confusion_matrix(y_true, preds)
        if cm.shape == (2, 2):
            tn, fp, fn, tp = cm.ravel()
        else:
            tn, fp, fn, tp = len(y_true), 0, 0, 0
        fpr = fp / max(fp + tn, 1)
        fps.append(fpr)
    # Find threshold where FPR closest to target
    fps = np.array(fps)
    closest_idx = np.argmin(np.abs(fps - target_fpr))
    optimal_t   = float(thresholds[closest_idx])
    log.info(f"Calibrated threshold: {optimal_t:.4f} (target FPR={target_fpr:.3%}, actual FPR={fps[closest_idx]:.3%})")
    return optimal_t

ALERT_THRESHOLD = calibrate_threshold(y_test.values, ENSEMBLE_SCORES, TARGET_FPR)

with mlflow.start_run(run_name="Fraud_Threshold_Calibration", nested=True):
    mlflow.log_metric("alert_threshold", ALERT_THRESHOLD)
    mlflow.log_metric("target_fpr",      TARGET_FPR)
    ensemble_auc = roc_auc_score(y_test, ENSEMBLE_SCORES)
    mlflow.log_metric("ensemble_auc",    ensemble_auc)

log.info(f"Ensemble AUC: {ensemble_auc:.4f} | Alert threshold: {ALERT_THRESHOLD:.4f}")

# COMMAND ----------

# -- ## 7 — Structured Streaming: Real-Time Fraud Scoring

# COMMAND ----------

# Broadcast models to workers
gbm_broadcast    = spark.sparkContext.broadcast(GBM_MODEL)
iso_broadcast    = spark.sparkContext.broadcast(iso_forest)
scaler_broadcast = spark.sparkContext.broadcast(scaler)
threshold_bc     = spark.sparkContext.broadcast(ALERT_THRESHOLD)
features_bc      = spark.sparkContext.broadcast(AVAIL_FEATURES)

def score_batch(batch_df, batch_id):
    """
    Micro-batch UDF: score each batch of streaming card transactions.
    Writes fraud scores to silver_card_transactions and alerts to gold.fact_fraud_event.
    """
    if batch_df.isEmpty():
        return

    batch_df = build_velocity_features(batch_df)
    pdf      = batch_df.toPandas()

    feat_cols  = features_bc.value
    avail      = [f for f in feat_cols if f in pdf.columns]
    X_batch    = pdf[avail].fillna(0)

    # Isolation Forest score
    X_scaled   = scaler_broadcast.value.transform(X_batch)
    iso_sc     = -iso_broadcast.value.score_samples(X_scaled)
    iso_sc     = (iso_sc - iso_sc.min()) / (iso_sc.max() - iso_sc.min() + 1e-10)

    # GBM score
    gbm_sc     = gbm_broadcast.value.predict_proba(X_batch)[:, 1]

    # Ensemble
    ens_sc     = 0.4 * iso_sc + 0.6 * gbm_sc

    pdf["fraud_score"]  = ens_sc
    pdf["fraud_alert"]  = (ens_sc >= threshold_bc.value).astype(bool)

    # Write scores back to silver (UPDATE approach via Delta)
    scores_sdf = spark.createDataFrame(
        pdf[["card_txn_id", "fraud_score", "fraud_alert"]]
    )
    target = DeltaTable.forName(spark, f"`{SILVER}`.`silver_card_transactions`")
    (
        target.alias("t")
              .merge(scores_sdf.alias("s"), "t.card_txn_id = s.card_txn_id")
              .whenMatchedUpdate(set={
                  "fraud_score": "s.fraud_score",
                  "fraud_alert": "s.fraud_alert",
              })
              .execute()
    )

    # Write confirmed alerts to gold.fact_fraud_event
    alerts_pdf = pdf[pdf["fraud_alert"]].copy()
    if len(alerts_pdf) > 0:
        alerts_sdf = spark.createDataFrame(alerts_pdf[
            ["card_txn_id","customer_id","account_id","txn_date","txn_amount_zar","fraud_score"]
        ]).withColumn("fraud_label",     F.lit("SUSPECTED")) \
          .withColumn("detection_model", F.lit("ENSEMBLE_IFO_GBM")) \
          .withColumn("alert_raised_at", F.current_timestamp()) \
          .withColumn("resolved_at",     F.lit(None).cast("timestamp")) \
          .withColumn("_gold_loaded_at", F.current_timestamp())

        alerts_sdf.write.format("delta").mode("append") \
                  .saveAsTable(f"`{GOLD}`.`fact_fraud_event`")

    log.info(f"Batch {batch_id}: {len(pdf):,} txns scored | {len(alerts_pdf):,} alerts raised")


# ── Start streaming job (runs continuously in production; trigger once in batch mode) ──
streaming_query = (
    spark.readStream
         .format("delta")
         .option("readChangeFeed", "true")
         .table(f"`{SILVER}`.`silver_card_transactions`")
         .filter(F.lit(True))  # score all records; fraud_score added by this notebook
         .writeStream
         .foreachBatch(score_batch)
         .outputMode("update")
         .option("checkpointLocation", CHECKPOINT_PATH)
         .trigger(availableNow=True)
         .start()
)

streaming_query.awaitTermination(timeout=1800)

# COMMAND ----------

# -- ## 8 — Fraud Dashboard Metrics

# COMMAND ----------

fraud_summary = spark.sql(f"""
    SELECT
        txn_date,
        COUNT(*)                                       AS total_transactions,
        SUM(CASE WHEN fraud_label = 'SUSPECTED' THEN 1 ELSE 0 END) AS suspected_fraud,
        SUM(CASE WHEN fraud_label = 'CONFIRMED' THEN 1 ELSE 0 END) AS confirmed_fraud,
        SUM(txn_amount_zar)                            AS total_amount_zar,
        SUM(CASE WHEN fraud_label IN ('SUSPECTED','CONFIRMED') THEN txn_amount_zar ELSE 0 END)
                                                       AS fraud_amount_zar
    FROM `{GOLD}`.`fact_fraud_event`
    WHERE txn_date >= current_date() - 30
    GROUP BY txn_date
    ORDER BY txn_date DESC
""")
display(fraud_summary)

print(f"\n05_fraud_detection — COMPLETE  (run_id={RUN_ID})")
print(f"Ensemble AUC: {ensemble_auc:.4f} | Alert threshold: {ALERT_THRESHOLD:.4f}")
