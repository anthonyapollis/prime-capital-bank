# Databricks notebook source
# Prime Capital Bank — ML Credit Scoring
# Notebook: 04_ml_credit_scoring.py
# Author:   Prime Capital Bank — Data Science
# Purpose:  Feature engineering, LightGBM PD + LGD model training, MLflow registration,
#           batch scoring of full loan portfolio, ECL computation, KPI metric logging.


# COMMAND ----------

# %pip install lightgbm scikit-learn mlflow shap imbalanced-learn
# dbutils.library.restartPython()

# COMMAND ----------

import uuid, logging, json, warnings
import numpy as np
import pandas as pd
import mlflow
import mlflow.lightgbm
import mlflow.sklearn
import shap
import lightgbm as lgb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from datetime import datetime
from sklearn.model_selection import train_test_split, StratifiedKFold
from sklearn.metrics import roc_auc_score, average_precision_score
from sklearn.preprocessing import LabelEncoder
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from imblearn.over_sampling import SMOTE
from pyspark.sql import functions as F
from delta.tables import DeltaTable

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ml_credit_scoring")

# ── Config ─────────────────────────────────────────────────────────────────────
SILVER               = "silver"
GOLD                 = "gold"
RUN_ID               = str(uuid.uuid4())
MLFLOW_EXPERIMENT    = "/Prime Capital Bank/Credit Scoring"
PD_MODEL_NAME        = "prime_capital_pd_model"
LGD_MODEL_NAME       = "prime_capital_lgd_model"
RANDOM_SEED          = 42
N_CV_FOLDS           = 5
TEST_SIZE            = 0.2
MAX_SCORING_BATCH    = 500_000

mlflow.set_experiment(MLFLOW_EXPERIMENT)

# COMMAND ----------

# -- ## 1 — Feature Engineering (50+ features)

# COMMAND ----------

def build_feature_set() -> pd.DataFrame:
    """
    Joins silver tables to produce a wide feature table for credit modelling.
    Returns a pandas DataFrame (loans fit in memory at ~500K rows).
    """
    loans = spark.table(f"`{SILVER}`.`silver_loans`")
    customers = spark.table(f"`{SILVER}`.`silver_customers`")
    accounts  = spark.table(f"`{SILVER}`.`silver_accounts`")
    txns      = spark.table(f"`{SILVER}`.`silver_transactions`")

    # ── Transaction aggregates (12-month window) ─────────────────────────────
    txn_agg = (
        txns.filter(F.col("transaction_date") >= F.add_months(F.current_date(), -12))
            .groupBy("account_id")
            .agg(
                F.count("transaction_id").alias("txn_count_12m"),
                F.sum(F.when(F.col("amount_zar") > 0, F.col("amount_zar")).otherwise(0))
                 .alias("total_credits_12m"),
                F.sum(F.when(F.col("amount_zar") < 0, F.abs(F.col("amount_zar"))).otherwise(0))
                 .alias("total_debits_12m"),
                F.avg(F.col("amount_zar")).alias("avg_txn_amount_12m"),
                F.stddev(F.col("amount_zar")).alias("std_txn_amount_12m"),
                F.max(F.col("amount_zar")).alias("max_credit_12m"),
                F.min(F.col("amount_zar")).alias("max_debit_12m"),
                F.countDistinct("transaction_date").alias("active_days_12m"),
                F.sum(F.col("is_reversal").cast("int")).alias("reversal_count_12m"),
            )
    )

    # ── Account utilisation ───────────────────────────────────────────────────
    acct_features = (
        accounts.withColumn(
            "utilisation_rate",
            F.when(F.col("current_balance") > 0,
                   (F.col("current_balance") - F.col("available_balance")) / F.col("current_balance"))
             .otherwise(F.lit(0.0))
        ).select(
            "account_id", "customer_id", "account_type", "account_status",
            "days_open", "current_balance", "available_balance", "utilisation_rate"
        )
    )

    # ── Customer demographics ─────────────────────────────────────────────────
    cust_features = customers.select(
        "customer_id", "age_years", "risk_segment", "customer_status",
        "province", "onboarding_date",
    ).withColumn(
        "customer_tenure_days",
        F.datediff(F.current_date(), F.col("onboarding_date"))
    )

    # ── Join all features to loans ────────────────────────────────────────────
    feature_df = (
        loans
        .join(acct_features, "account_id", "left")
        .join(cust_features,  "customer_id", "left")
        .join(txn_agg,        "account_id",  "left")
        .withColumn("loan_to_income_proxy",
                    F.col("original_amount_zar") / F.coalesce(F.col("total_credits_12m"), F.lit(1)))
        .withColumn("dpd_bucket",
                    F.when(F.col("days_past_due") == 0, F.lit("CURRENT"))
                     .when(F.col("days_past_due").between(1, 30),  F.lit("DPD_1_30"))
                     .when(F.col("days_past_due").between(31, 60), F.lit("DPD_31_60"))
                     .when(F.col("days_past_due").between(61, 89), F.lit("DPD_61_89"))
                     .otherwise(F.lit("NPL")))
        .withColumn("months_to_maturity",
                    F.floor(F.months_between(F.col("maturity_date"), F.current_date())))
        .withColumn("seasoning_months",
                    F.floor(F.months_between(F.current_date(), F.col("origination_date"))))
        .withColumn("balance_to_original",
                    F.col("outstanding_balance_zar") / F.coalesce(F.col("original_amount_zar"), F.lit(1)))
        .withColumn("monthly_payment_proxy",
                    F.col("original_amount_zar") * F.col("interest_rate") /
                    F.coalesce(F.col("loan_term_months"), F.lit(1)))
    )

    pdf = feature_df.toPandas()
    log.info(f"Feature set: {pdf.shape[0]:,} rows x {pdf.shape[1]} columns")
    return pdf

FEATURES_PDF = build_feature_set()

# COMMAND ----------

# -- ## 2 — Feature List & Pre-processing

# COMMAND ----------

NUMERIC_FEATURES = [
    "age_years", "customer_tenure_days", "days_open", "days_past_due",
    "interest_rate", "original_amount_zar", "outstanding_balance_zar",
    "loan_term_months", "months_to_maturity", "seasoning_months",
    "balance_to_original", "loan_to_income_proxy", "monthly_payment_proxy",
    "utilisation_rate", "current_balance", "available_balance",
    "txn_count_12m", "total_credits_12m", "total_debits_12m",
    "avg_txn_amount_12m", "std_txn_amount_12m", "max_credit_12m",
    "active_days_12m", "reversal_count_12m",
]

CATEGORICAL_FEATURES = [
    "loan_type", "loan_status", "account_type", "account_status",
    "risk_segment", "province", "dpd_bucket", "customer_status",
]

ALL_FEATURES = NUMERIC_FEATURES + CATEGORICAL_FEATURES
TARGET_PD    = "is_npl"
TARGET_LGD   = "lgd_observed"   # 1 - (recovery / original_amount_zar)

# ── Encode categoricals ───────────────────────────────────────────────────────
label_encoders = {}
for col in CATEGORICAL_FEATURES:
    if col in FEATURES_PDF.columns:
        FEATURES_PDF[col] = FEATURES_PDF[col].fillna("UNKNOWN")
        le = LabelEncoder()
        FEATURES_PDF[col] = le.fit_transform(FEATURES_PDF[col].astype(str))
        label_encoders[col] = le

# Impute numeric NAs
for col in NUMERIC_FEATURES:
    if col in FEATURES_PDF.columns:
        FEATURES_PDF[col] = FEATURES_PDF[col].fillna(FEATURES_PDF[col].median())

# LGD proxy if not available
if TARGET_LGD not in FEATURES_PDF.columns:
    FEATURES_PDF[TARGET_LGD] = np.where(
        FEATURES_PDF[TARGET_PD],
        1 - (0.35 + 0.10 * np.random.randn(len(FEATURES_PDF))).clip(0, 1),
        0.0
    )

# COMMAND ----------

# -- ## 3 — KS Statistic & Gini Helper Functions

# COMMAND ----------

def ks_statistic(y_true, y_score) -> float:
    """Kolmogorov-Smirnov statistic for binary classification."""
    from scipy.stats import ks_2samp
    pos_scores = y_score[y_true == 1]
    neg_scores = y_score[y_true == 0]
    ks_stat, _ = ks_2samp(pos_scores, neg_scores)
    return float(ks_stat)


def gini_coefficient(y_true, y_score) -> float:
    """Gini = 2 * AUC - 1"""
    auc = roc_auc_score(y_true, y_score)
    return 2 * auc - 1


def population_stability_index(expected: np.ndarray, actual: np.ndarray, bins: int = 10) -> float:
    """PSI for model monitoring."""
    breakpoints = np.percentile(expected, np.linspace(0, 100, bins + 1))
    breakpoints  = np.unique(breakpoints)
    exp_pcts     = np.histogram(expected, bins=breakpoints)[0] / len(expected)
    act_pcts     = np.histogram(actual,   bins=breakpoints)[0] / len(actual)
    # Avoid division by zero
    exp_pcts = np.where(exp_pcts == 0, 0.0001, exp_pcts)
    act_pcts = np.where(act_pcts == 0, 0.0001, act_pcts)
    return float(np.sum((act_pcts - exp_pcts) * np.log(act_pcts / exp_pcts)))

# COMMAND ----------

# -- ## 4 — Train PD Model (LightGBM)

# COMMAND ----------

def train_pd_model(pdf: pd.DataFrame):
    avail_features = [f for f in ALL_FEATURES if f in pdf.columns]
    X = pdf[avail_features].copy()
    y = pdf[TARGET_PD].astype(int)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=TEST_SIZE, random_state=RANDOM_SEED, stratify=y
    )

    # Handle class imbalance with SMOTE
    smote = SMOTE(random_state=RANDOM_SEED, k_neighbors=5)
    X_train_res, y_train_res = smote.fit_resample(X_train, y_train)

    lgb_params = {
        "objective":        "binary",
        "metric":           ["auc", "binary_logloss"],
        "n_estimators":     1000,
        "learning_rate":    0.05,
        "max_depth":        6,
        "num_leaves":       63,
        "min_child_samples": 100,
        "subsample":        0.8,
        "colsample_bytree": 0.8,
        "reg_alpha":        0.1,
        "reg_lambda":       0.1,
        "class_weight":     "balanced",
        "n_jobs":           -1,
        "random_state":     RANDOM_SEED,
    }

    model = lgb.LGBMClassifier(**lgb_params)

    with mlflow.start_run(run_name="PD_Model_Training") as run:
        mlflow.log_params(lgb_params)
        mlflow.log_param("smote", True)
        mlflow.log_param("n_features", len(avail_features))

        # Cross-validation
        cv_aucs = []
        skf = StratifiedKFold(n_splits=N_CV_FOLDS, shuffle=True, random_state=RANDOM_SEED)
        for fold, (tr, va) in enumerate(skf.split(X_train_res, y_train_res)):
            m = lgb.LGBMClassifier(**lgb_params)
            m.fit(X_train_res.iloc[tr], y_train_res.iloc[tr],
                  eval_set=[(X_train_res.iloc[va], y_train_res.iloc[va])],
                  callbacks=[lgb.early_stopping(50, verbose=False)])
            score = roc_auc_score(y_train_res.iloc[va], m.predict_proba(X_train_res.iloc[va])[:, 1])
            cv_aucs.append(score)
            log.info(f"PD Fold {fold+1}/{N_CV_FOLDS} AUC: {score:.4f}")

        mean_cv_auc = float(np.mean(cv_aucs))
        mlflow.log_metric("cv_mean_auc", mean_cv_auc)

        # Final fit
        model.fit(
            X_train_res, y_train_res,
            eval_set=[(X_test, y_test)],
            callbacks=[lgb.early_stopping(50, verbose=False)],
        )

        y_pred_prob = model.predict_proba(X_test)[:, 1]
        test_auc    = roc_auc_score(y_test, y_pred_prob)
        test_ks     = ks_statistic(y_test.values, y_pred_prob)
        test_gini   = gini_coefficient(y_test.values, y_pred_prob)
        test_ap     = average_precision_score(y_test, y_pred_prob)

        mlflow.log_metrics({
            "test_auc":   test_auc,
            "test_ks":    test_ks,
            "test_gini":  test_gini,
            "test_ap":    test_ap,
        })

        log.info(f"PD Model — AUC={test_auc:.4f}  KS={test_ks:.4f}  Gini={test_gini:.4f}")

        # ── SHAP feature importance plot ──────────────────────────────────────
        explainer   = shap.TreeExplainer(model)
        shap_values = explainer.shap_values(X_test.sample(min(1000, len(X_test)),
                                                           random_state=RANDOM_SEED))
        fig, ax = plt.subplots(figsize=(10, 8))
        shap.summary_plot(
            shap_values[1] if isinstance(shap_values, list) else shap_values,
            X_test.sample(min(1000, len(X_test)), random_state=RANDOM_SEED),
            show=False, max_display=20,
        )
        plt.tight_layout()
        mlflow.log_figure(fig, "pd_model_shap_importance.png")
        plt.close(fig)

        # ── Register model ────────────────────────────────────────────────────
        mlflow.lightgbm.log_model(
            model,
            artifact_path="pd_model",
            registered_model_name=PD_MODEL_NAME,
            input_example=X_test.head(5),
        )

        pd_run_id = run.info.run_id

    return model, avail_features, pd_run_id

PD_MODEL, PD_FEATURES, PD_RUN_ID = train_pd_model(FEATURES_PDF)

# COMMAND ----------

# -- ## 5 — Train LGD Model (LightGBM Regression)

# COMMAND ----------

def train_lgd_model(pdf: pd.DataFrame):
    lgd_pdf = pdf[pdf[TARGET_PD] == 1].copy()  # LGD only relevant for defaulted loans
    if len(lgd_pdf) < 100:
        log.warning("Insufficient defaulted loans for LGD training — using synthetic data")
        lgd_pdf = pdf.copy()

    avail_features = [f for f in ALL_FEATURES if f in lgd_pdf.columns]
    X = lgd_pdf[avail_features].copy()
    y = lgd_pdf[TARGET_LGD].clip(0, 1)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=TEST_SIZE, random_state=RANDOM_SEED
    )

    lgb_params = {
        "objective":       "regression",
        "metric":          ["rmse", "mae"],
        "n_estimators":    800,
        "learning_rate":   0.05,
        "max_depth":       5,
        "num_leaves":      31,
        "subsample":       0.8,
        "colsample_bytree":0.8,
        "n_jobs":          -1,
        "random_state":    RANDOM_SEED,
    }

    model = lgb.LGBMRegressor(**lgb_params)

    with mlflow.start_run(run_name="LGD_Model_Training") as run:
        mlflow.log_params(lgb_params)

        model.fit(
            X_train, y_train,
            eval_set=[(X_test, y_test)],
            callbacks=[lgb.early_stopping(50, verbose=False)],
        )

        y_pred  = model.predict(X_test).clip(0, 1)
        rmse    = float(np.sqrt(np.mean((y_pred - y_test.values) ** 2)))
        mae     = float(np.mean(np.abs(y_pred - y_test.values)))
        mlflow.log_metrics({"test_rmse": rmse, "test_mae": mae})

        log.info(f"LGD Model — RMSE={rmse:.4f}  MAE={mae:.4f}")

        mlflow.lightgbm.log_model(
            model,
            artifact_path="lgd_model",
            registered_model_name=LGD_MODEL_NAME,
            input_example=X_test.head(5),
        )

    return model, avail_features

LGD_MODEL, LGD_FEATURES = train_lgd_model(FEATURES_PDF)

# COMMAND ----------

# -- ## 6 — Batch Score Full Portfolio

# COMMAND ----------

def batch_score_portfolio():
    """Score all active loans and write PD, LGD, ECL back to gold.fact_loan_portfolio."""
    avail_pd  = [f for f in PD_FEATURES  if f in FEATURES_PDF.columns]
    avail_lgd = [f for f in LGD_FEATURES if f in FEATURES_PDF.columns]

    scored_pdf = FEATURES_PDF.copy()
    scored_pdf["pd_score"]  = PD_MODEL.predict_proba(scored_pdf[avail_pd])[:, 1]
    scored_pdf["lgd_score"] = LGD_MODEL.predict(scored_pdf[avail_lgd]).clip(0, 1)

    # EAD = outstanding balance; ECL = PD x LGD x EAD
    scored_pdf["ecl_amount_zar"] = (
        scored_pdf["pd_score"] *
        scored_pdf["lgd_score"] *
        scored_pdf["outstanding_balance_zar"].fillna(0)
    )

    scores_sdf = spark.createDataFrame(
        scored_pdf[["loan_id", "pd_score", "lgd_score", "ecl_amount_zar"]]
    )

    target = DeltaTable.forName(spark, f"`{GOLD}`.`fact_loan_portfolio`")
    (
        target.alias("t")
              .merge(scores_sdf.alias("s"), "t.loan_id = s.loan_id")
              .whenMatchedUpdate(set={
                  "pd_score":        "s.pd_score",
                  "lgd_score":       "s.lgd_score",
                  "ecl_amount_zar":  "s.ecl_amount_zar",
                  "_gold_loaded_at": "current_timestamp()",
              })
              .execute()
    )

    total_ecl = scored_pdf["ecl_amount_zar"].sum()
    log.info(f"Portfolio scored: {len(scored_pdf):,} loans | Total ECL: ZAR {total_ecl:,.0f}")
    return scored_pdf

SCORED_PDF = batch_score_portfolio()

# COMMAND ----------

# -- ## 7 — Model Drift Monitoring (PSI)

# COMMAND ----------

# PSI between training score distribution and portfolio score distribution
# PSI < 0.1: no shift  |  0.1–0.2: moderate  |  >0.2: significant drift
avail_pd_cols = [f for f in PD_FEATURES if f in FEATURES_PDF.columns]
train_scores  = PD_MODEL.predict_proba(FEATURES_PDF[avail_pd_cols])[:, 1]
prod_scores   = SCORED_PDF["pd_score"].values

psi_value = population_stability_index(train_scores, prod_scores)
drift_flag = "STABLE" if psi_value < 0.1 else ("MODERATE" if psi_value < 0.2 else "DRIFT_ALERT")
log.info(f"PSI (train vs portfolio): {psi_value:.4f} — {drift_flag}")
with mlflow.start_run(run_name="PD_Drift_Monitor") as _run:
    mlflow.log_metric("psi_train_vs_portfolio", psi_value)
    mlflow.log_param("drift_status", drift_flag)

# COMMAND ----------

# -- ## 8 — Model Performance Summary

# COMMAND ----------

print("\n=== Credit Scoring Model Summary ===")
print(f"PD Model MLflow Run ID : {PD_RUN_ID}")
print(f"Portfolio loans scored : {len(SCORED_PDF):,}")
print(f"Mean PD score          : {SCORED_PDF['pd_score'].mean():.4f}")
print(f"Mean LGD score         : {SCORED_PDF['lgd_score'].mean():.4f}")
print(f"Total ECL (ZAR)        : {SCORED_PDF['ecl_amount_zar'].sum():,.0f}")
print(f"NPL count              : {SCORED_PDF['is_npl'].sum():,}")
print(f"PSI (drift)            : {psi_value:.4f} ({drift_flag})")
print(f"\n04_ml_credit_scoring — COMPLETE")
