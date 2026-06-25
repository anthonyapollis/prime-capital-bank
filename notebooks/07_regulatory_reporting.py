# Databricks notebook source
# Prime Capital Bank — Regulatory Reporting
# Notebook: 07_regulatory_reporting.py
# Author:   Prime Capital Bank — Finance & Regulatory
# Purpose:  Compute BA700 (Basel III capital adequacy), DI200 (domestic assets/liabilities),
#           NPL ratios, provision coverage. Export CSVs to ADLS for SARB submission.
#           Trigger Power BI dataset refresh via REST API.


# COMMAND ----------

import uuid, logging, json, requests
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("regulatory_reporting")
GOLD               = "gold"
SILVER             = "silver"
ADLS_ACCOUNT       = "primecapitaladls"
REPORT_DATE        = date.today().replace(day=1) - relativedelta(days=1)  # last day of previous month
REPORT_DATE_STR    = REPORT_DATE.strftime("%Y%m%d")
REPORT_PERIOD      = REPORT_DATE.strftime("%Y-%m")
RUN_ID             = str(uuid.uuid4())

# Basel III minimum ratios (SARB Directive D1/2023 aligned)
SARB_MIN_CET1      = 0.045    # 4.5% Common Equity Tier 1
SARB_MIN_TIER1     = 0.060    # 6.0% Tier 1
SARB_MIN_TOTAL_CAR = 0.080    # 8.0% Total Capital Adequacy
CAPITAL_BUFFER     = 0.025    # 2.5% Capital Conservation Buffer
SARB_REQUIRED_CAR  = SARB_MIN_TOTAL_CAR + CAPITAL_BUFFER  # 10.5% effective minimum

# COMMAND ----------

# -- ## 1 — Load Gold Tables

# COMMAND ----------

loans    = spark.table(f"`{GOLD}`.`fact_loan_portfolio`")
accounts = spark.table(f"`{GOLD}`.`dim_account`")
txns     = spark.table(f"`{GOLD}`.`fact_transaction`")

# Filter to report date snapshot
loans_snap = loans.filter(F.col("_gold_loaded_at") <= F.lit(REPORT_DATE.isoformat()))

# COMMAND ----------

# -- ## 2 — NPL Ratios and Provision Coverage

# COMMAND ----------

def compute_npl_metrics():
    metrics = loans_snap.agg(
        F.sum("outstanding_balance_zar").alias("total_gross_loans"),
        F.sum(F.when(F.col("is_npl"), F.col("outstanding_balance_zar"))
               .otherwise(F.lit(0))).alias("npl_gross"),
        F.sum("ecl_amount_zar").alias("total_provisions"),
        F.count("loan_id").alias("total_loans"),
        F.count(F.when(F.col("is_npl"), F.lit(1))).alias("npl_count"),
    ).collect()[0]

    total_gross    = float(metrics["total_gross_loans"] or 0)
    npl_gross      = float(metrics["npl_gross"] or 0)
    provisions     = float(metrics["total_provisions"] or 0)
    npl_ratio      = npl_gross / max(total_gross, 1)
    coverage_ratio = provisions / max(npl_gross, 1)

    log.info(f"NPL Ratio: {npl_ratio:.2%} | Provision Coverage: {coverage_ratio:.2%}")

    npl_df = spark.createDataFrame([{
        "report_period":    REPORT_PERIOD,
        "report_date":      REPORT_DATE_STR,
        "total_gross_loans_zar": total_gross,
        "npl_gross_zar":    npl_gross,
        "npl_count":        int(metrics["npl_count"] or 0),
        "total_loans":      int(metrics["total_loans"] or 0),
        "npl_ratio_pct":    round(npl_ratio * 100, 2),
        "total_provisions_zar": provisions,
        "provision_coverage_pct": round(coverage_ratio * 100, 2),
        "run_id":           RUN_ID,
    }])

    return npl_df, npl_ratio, coverage_ratio

NPL_DF, NPL_RATIO, COVERAGE_RATIO = compute_npl_metrics()

# COMMAND ----------

# -- ## 3 — BA700: Basel III Capital Adequacy Return

# COMMAND ----------

def compute_ba700():
    """
    Compute simplified BA700 return figures.
    In production these values come from the regulatory capital system;
    here we derive proxies from the loan and account data.
    """
    # Risk-Weighted Assets (simplified: corporate loans 100%, retail 75%, secured 50%)
    rwa_df = loans_snap.withColumn(
        "risk_weight",
        F.when(F.col("loan_type") == "CORPORATE", F.lit(1.00))
         .when(F.col("loan_type") == "HOME_LOAN",  F.lit(0.50))
         .when(F.col("loan_type") == "RETAIL",      F.lit(0.75))
         .when(F.col("loan_type") == "SME",         F.lit(0.85))
         .otherwise(F.lit(1.00))
    ).withColumn("rwa", F.col("outstanding_balance_zar") * F.col("risk_weight"))

    total_rwa = float(rwa_df.agg(F.sum("rwa")).collect()[0][0] or 0)
    total_ecl = float(loans_snap.agg(F.sum("ecl_amount_zar")).collect()[0][0] or 0)

    # Capital: sourced from a capital register table (stub values used here)
    try:
        capital = spark.table(f"`{GOLD}`.`capital_register`") \
                       .filter(F.col("report_date") == F.lit(REPORT_DATE.isoformat())) \
                       .agg(
                           F.sum("cet1_capital").alias("cet1"),
                           F.sum("at1_capital").alias("at1"),
                           F.sum("tier2_capital").alias("t2"),
                       ).collect()[0]
        cet1 = float(capital["cet1"] or 0)
        at1  = float(capital["at1"]  or 0)
        t2   = float(capital["t2"]   or 0)
    except Exception:
        log.warning("capital_register not found — using stub capital values")
        cet1 = total_rwa * 0.12
        at1  = total_rwa * 0.015
        t2   = total_rwa * 0.02

    tier1_capital  = cet1 + at1
    total_capital  = tier1_capital + t2

    cet1_ratio     = cet1 / max(total_rwa, 1)
    tier1_ratio    = tier1_capital / max(total_rwa, 1)
    total_car      = total_capital / max(total_rwa, 1)

    log.info(f"BA700 — CET1: {cet1_ratio:.2%} | Tier1: {tier1_ratio:.2%} | CAR: {total_car:.2%}")

    if total_car < SARB_REQUIRED_CAR:
        log.error(f"CAPITAL BREACH: CAR {total_car:.2%} < required {SARB_REQUIRED_CAR:.2%}")
    if cet1_ratio < SARB_MIN_CET1:
        log.error(f"CAPITAL BREACH: CET1 {cet1_ratio:.2%} < required {SARB_MIN_CET1:.2%}")

    ba700_data = {
        "report_period":            REPORT_PERIOD,
        "report_date":              REPORT_DATE_STR,
        "total_risk_weighted_assets_zar": round(total_rwa, 2),
        "cet1_capital_zar":         round(cet1, 2),
        "at1_capital_zar":          round(at1, 2),
        "tier1_capital_zar":        round(tier1_capital, 2),
        "tier2_capital_zar":        round(t2, 2),
        "total_regulatory_capital_zar": round(total_capital, 2),
        "cet1_ratio_pct":           round(cet1_ratio * 100, 2),
        "tier1_ratio_pct":          round(tier1_ratio * 100, 2),
        "total_car_pct":            round(total_car * 100, 2),
        "sarb_min_cet1_pct":        round(SARB_MIN_CET1 * 100, 2),
        "sarb_min_tier1_pct":       round(SARB_MIN_TIER1 * 100, 2),
        "sarb_required_car_pct":    round(SARB_REQUIRED_CAR * 100, 2),
        "capital_headroom_zar":     round((total_car - SARB_REQUIRED_CAR) * total_rwa, 2),
        "total_ecl_provisions_zar": round(total_ecl, 2),
        "run_id":                   RUN_ID,
    }

    ba700_df = spark.createDataFrame([ba700_data])
    return ba700_df, total_car

BA700_DF, TOTAL_CAR = compute_ba700()

# COMMAND ----------

# -- ## 4 — DI200: Domestic Assets and Liabilities

# COMMAND ----------

def compute_di200():
    """
    DI200 return: summary of domestic balance sheet positions.
    """
    acct_balances = accounts.filter(F.col("scd_is_current"))

    assets = loans_snap.agg(
        F.sum("outstanding_balance_zar").alias("total_loans_zar"),
    ).collect()[0]

    liabilities = acct_balances.filter(
        F.col("account_type").isin("SAVINGS", "CURRENT", "CALL", "FIXED_DEPOSIT")
    ).agg(
        F.sum("current_balance").alias("total_deposits_zar"),
        F.count("account_id").alias("total_accounts"),
    ).collect()[0]

    total_loans    = float(assets["total_loans_zar"]   or 0)
    total_deposits = float(liabilities["total_deposits_zar"] or 0)
    loan_to_deposit = total_loans / max(total_deposits, 1)

    log.info(f"DI200 — Loans: ZAR {total_loans:,.0f} | Deposits: ZAR {total_deposits:,.0f} | L/D: {loan_to_deposit:.2%}")

    di200_data = {
        "report_period":              REPORT_PERIOD,
        "report_date":                REPORT_DATE_STR,
        "total_loans_domestic_zar":   round(total_loans, 2),
        "total_deposits_domestic_zar":round(total_deposits, 2),
        "loan_to_deposit_ratio_pct":  round(loan_to_deposit * 100, 2),
        "total_accounts":             int(liabilities["total_accounts"] or 0),
        "run_id":                     RUN_ID,
    }

    di200_df = spark.createDataFrame([di200_data])
    return di200_df

DI200_DF = compute_di200()

# COMMAND ----------

# -- ## 5 — Export to ADLS as CSV

# COMMAND ----------

def export_csv(df, report_name: str):
    """Write a single CSV file (coalesce to 1 partition) to ADLS gold/regulatory/."""
    path = f"{OUTPUT_BASE}/{report_name}_{REPORT_DATE_STR}.csv"
    df.coalesce(1) \
      .write.format("csv") \
      .option("header", "true") \
      .mode("overwrite") \
      .save(path)
    log.info(f"Exported {report_name} -> {path}")
    return path

NPL_PATH   = export_csv(NPL_DF,   "npl_metrics")
BA700_PATH = export_csv(BA700_DF, "ba700_capital_adequacy")
DI200_PATH = export_csv(DI200_DF, "di200_domestic_bal_sheet")

# Also save to Delta for history
for df, tbl in [
    (NPL_DF,   "reg_npl_metrics"),
    (BA700_DF, "reg_ba700"),
    (DI200_DF, "reg_di200"),
]:
    df.write.format("delta").mode("append").option("mergeSchema", "true") \
      .saveAsTable(f"`{GOLD}`.`{tbl}`")

# COMMAND ----------

# -- ## 6 — Power BI Dataset Refresh

# COMMAND ----------

def trigger_powerbi_refresh():
    """
    Trigger Power BI dataset refresh via REST API.
    Requires Service Principal credentials stored in Databricks Secret Scope.
    """
    try:
        tenant_id    = dbutils.secrets.get("prime-capital-secrets", "aad-tenant-id")
        client_id    = dbutils.secrets.get("prime-capital-secrets", "pbi-sp-client-id")
        client_secret= dbutils.secrets.get("prime-capital-secrets", "pbi-sp-client-secret")
        workspace_id = dbutils.secrets.get("prime-capital-secrets", "pbi-workspace-id")
        dataset_id   = dbutils.secrets.get("prime-capital-secrets", "pbi-dataset-id")

        # Obtain OAuth token
        token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
        token_resp = requests.post(token_url, data={
            "grant_type":    "client_credentials",
            "client_id":     client_id,
            "client_secret": client_secret,
            "scope":         "https://analysis.windows.net/powerbi/api/.default",
        }, timeout=30)
        token_resp.raise_for_status()
        access_token = token_resp.json()["access_token"]

        # Trigger refresh
        refresh_url = (
            f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}"
            f"/datasets/{dataset_id}/refreshes"
        )
        refresh_resp = requests.post(
            refresh_url,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type":  "application/json",
            },
            json={"notifyOption": "MailOnCompletion"},
            timeout=30,
        )
        refresh_resp.raise_for_status()
        log.info(f"Power BI refresh triggered — status {refresh_resp.status_code}")

    except Exception as e:
        log.warning(f"Power BI refresh failed (non-fatal): {e}")

trigger_powerbi_refresh()

# COMMAND ----------

# -- ## 7 — Regulatory Summary

# COMMAND ----------

print("\n" + "=" * 60)
print(f"  PRIME CAPITAL BANK — REGULATORY REPORT SUMMARY")
print(f"  Report Period : {REPORT_PERIOD}")
print(f"  Generated     : {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
print("=" * 60)
print(f"\n  BA700 — Capital Adequacy Return")
print(f"    Total Capital Adequacy Ratio : {TOTAL_CAR:.2%}")
print(f"    SARB Minimum Required CAR    : {SARB_REQUIRED_CAR:.2%}")
print(f"    Status : {'COMPLIANT' if TOTAL_CAR >= SARB_REQUIRED_CAR else '*** BREACH ***'}")
print(f"\n  NPL — Non-Performing Loans")
print(f"    NPL Ratio        : {NPL_RATIO:.2%}")
print(f"    Provision Cover  : {COVERAGE_RATIO:.2%}")
print(f"\n  Output files")
print(f"    {NPL_PATH}")
print(f"    {BA700_PATH}")
print(f"    {DI200_PATH}")
print(f"\n07_regulatory_reporting — COMPLETE")
