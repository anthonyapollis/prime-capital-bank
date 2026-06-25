# Databricks notebook source
# Prime Capital Bank — Bronze Layer: Synthetic Data Generation
# Generates realistic banking data and writes to Delta bronze tables.
# Column names are aligned to silver transformation expectations.

# COMMAND ----------

import uuid, logging
from datetime import datetime
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bronze_ingestion")

RUN_ID      = str(uuid.uuid4())
RUN_STARTED = datetime.utcnow()

print(f"Starting Bronze Ingestion -- {RUN_STARTED.isoformat()}Z")
print(f"Run ID: {RUN_ID}")

# COMMAND ----------
# -- 1: Control table helpers

def log_start(table_name):
    spark.sql(f"""
        INSERT INTO bronze.pipeline_control
        VALUES ('{RUN_ID}','01_bronze_ingestion','{table_name}','RUNNING',
                NULL, NULL, current_timestamp(), NULL)
    """)

def log_end(table_name, rows, status="SUCCESS", error=None):
    # Escape single quotes to prevent SQL parse errors in error messages
    err = f"'{error[:400].replace(chr(39), chr(39)*2)}'" if error else "NULL"
    spark.sql(f"""
        UPDATE bronze.pipeline_control
        SET run_status='{status}', rows_ingested={rows},
            error_message={err}, completed_at=current_timestamp()
        WHERE run_id='{RUN_ID}' AND table_name='{table_name}'
    """)

# COMMAND ----------
# -- 2: Customers (50 000 rows)
# Columns aligned to silver: id_number, email_address, mobile_number, tax_number,
#                             risk_segment, customer_status, street_address, suburb,
#                             province, postal_code

log_start("bronze_customers")
try:
    df = spark.range(1, 50_001).select(
        F.concat(F.lit("CUST-"), F.lpad(F.col("id").cast("string"), 6, "0")).alias("customer_id"),
        F.concat(F.lit("First"), F.col("id").cast("string")).alias("first_name"),
        F.concat(F.lit("Last"),  F.col("id").cast("string")).alias("last_name"),
        F.when(F.col("id") % 2 == 0, "M").otherwise("F").alias("gender"),
        F.date_sub(F.current_date(), (F.col("id") % 18000 + 6570).cast("int")).alias("date_of_birth"),
        F.concat(F.lit("90"), F.lpad((F.col("id") % 10000000000).cast("string"), 11, "0")).alias("id_number"),
        F.concat(F.lit("TAX"), F.lpad(F.col("id").cast("string"), 9, "0")).alias("tax_number"),
        F.when(F.col("id") % 10 == 0, "CORPORATE").otherwise("RETAIL").alias("customer_segment"),
        F.when(F.col("id") % 5 == 0, "HIGH_RISK")
         .when(F.col("id") % 3 == 0, "MEDIUM_RISK")
         .otherwise("LOW_RISK").alias("risk_segment"),
        F.concat(F.lit("customer"), F.col("id").cast("string"), F.lit("@example.co.za")).alias("email_address"),
        F.concat(F.lit("0"), F.lpad((F.col("id") % 900_000_000 + 100_000_000).cast("string"), 9, "0")).alias("mobile_number"),
        F.concat(F.lit("Unit "), F.col("id").cast("string"), F.lit(" Main Road")).alias("street_address"),
        F.array(F.lit("Sandton"), F.lit("Rondebosch"), F.lit("Umhlanga"), F.lit("Centurion"))
          .getItem((F.col("id") % 4).cast("int")).alias("suburb"),
        F.array(F.lit("Johannesburg"), F.lit("Cape Town"), F.lit("Durban"), F.lit("Pretoria"))
          .getItem((F.col("id") % 4).cast("int")).alias("city"),
        F.array(F.lit("GAUTENG"), F.lit("WESTERN CAPE"), F.lit("KWAZULU-NATAL"), F.lit("GAUTENG"))
          .getItem((F.col("id") % 4).cast("int")).alias("province"),
        F.lpad((F.col("id") % 9000 + 1000).cast("string"), 4, "0").alias("postal_code"),
        F.lit("South Africa").alias("country"),
        F.date_sub(F.current_date(), (F.col("id") % 3650).cast("int")).alias("onboarding_date"),
        F.when(F.col("id") % 50 == 0, "DORMANT")
         .when(F.col("id") % 20 == 0, "SUSPENDED")
         .otherwise("ACTIVE").alias("customer_status"),
        F.lit(RUN_ID).alias("_ingestion_run_id"),
        F.current_timestamp().alias("_ingestion_timestamp"),
    )
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze.bronze_customers")
    cnt = df.count()
    log_end("bronze_customers", cnt)
    log.info(f"bronze_customers: {cnt:,} rows")
except Exception as e:
    log_end("bronze_customers", 0, "FAILED", str(e)); raise

# COMMAND ----------
# -- 3: Accounts (80 000 rows)
# Columns aligned to silver: current_balance, available_balance, close_date,
#                             account_status, currency_code

log_start("bronze_accounts")
try:
    df = spark.range(1, 80_001).select(
        F.concat(F.lit("ACC-"), F.lpad(F.col("id").cast("string"), 8, "0")).alias("account_id"),
        F.concat(F.lit("CUST-"), F.lpad((F.col("id") % 50_000 + 1).cast("string"), 6, "0")).alias("customer_id"),
        F.when(F.col("id") % 5 == 0, "HOME_LOAN")
         .when(F.col("id") % 4 == 0, "VEHICLE_FINANCE")
         .when(F.col("id") % 3 == 0, "SAVINGS")
         .when(F.col("id") % 2 == 0, "CHEQUE")
         .otherwise("CREDIT_CARD").alias("account_type"),
        F.round((F.col("id") * 317.53 % 500_000) + 100, 2).alias("current_balance"),
        F.round((F.col("id") * 289.11 % 500_000) + 50,  2).alias("available_balance"),
        F.lit("ZAR").alias("currency_code"),
        F.when(F.col("id") % 30 == 0, "CLOSED")
         .when(F.col("id") % 15 == 0, "FROZEN")
         .otherwise("ACTIVE").alias("account_status"),
        F.date_sub(F.current_date(), (F.col("id") % 1825).cast("int")).alias("open_date"),
        F.when(F.col("id") % 30 == 0,
               F.date_sub(F.current_date(), (F.col("id") % 365).cast("int"))
        ).otherwise(F.lit(None).cast("date")).alias("close_date"),
        F.round(F.col("id") % 200 / 10.0, 2).alias("interest_rate"),
        F.lit(RUN_ID).alias("_ingestion_run_id"),
        F.current_timestamp().alias("_ingestion_timestamp"),
    )
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze.bronze_accounts")
    cnt = df.count()
    log_end("bronze_accounts", cnt)
    log.info(f"bronze_accounts: {cnt:,} rows")
except Exception as e:
    log_end("bronze_accounts", 0, "FAILED", str(e)); raise

# COMMAND ----------
# -- 4: Transactions (500 000 rows)
# Columns aligned to silver: currency_code, transaction_status, channel, transaction_ts

log_start("bronze_transactions")
try:
    df = spark.range(1, 500_001).select(
        F.concat(F.lit("TXN-"), F.lpad(F.col("id").cast("string"), 9, "0")).alias("transaction_id"),
        F.concat(F.lit("ACC-"), F.lpad((F.col("id") % 80_000 + 1).cast("string"), 8, "0")).alias("account_id"),
        F.concat(F.lit("CUST-"), F.lpad(((F.col("id") % 80_000 + 1) % 50_000 + 1).cast("string"), 6, "0")).alias("customer_id"),
        F.when(F.col("id") % 6 == 0, "PURCHASE")
         .when(F.col("id") % 5 == 0, "TRANSFER")
         .when(F.col("id") % 4 == 0, "ATM_WITHDRAWAL")
         .when(F.col("id") % 3 == 0, "DIRECT_DEPOSIT")
         .when(F.col("id") % 2 == 0, "DEBIT_ORDER")
         .otherwise("REVERSAL").alias("transaction_type"),
        F.round((F.col("id") * 73.19 % 50_000) + 1, 2).alias("amount"),
        F.lit("ZAR").alias("currency_code"),
        F.to_date(F.timestamp_seconds(
            (F.unix_timestamp(F.current_timestamp()) - F.col("id") % (365 * 24 * 3600)).cast("long")
        )).alias("transaction_date"),
        F.timestamp_seconds(
            (F.unix_timestamp(F.current_timestamp()) - F.col("id") % (365 * 24 * 3600)).cast("long")
        ).alias("transaction_ts"),
        F.when(F.col("id") % 200 == 0, "REVERSED")
         .when(F.col("id") % 50  == 0, "PENDING")
         .otherwise("COMPLETED").alias("transaction_status"),
        F.when(F.col("id") % 4 == 0, "ATM")
         .when(F.col("id") % 3 == 0, "ONLINE")
         .when(F.col("id") % 2 == 0, "BRANCH")
         .otherwise("MOBILE").alias("channel"),
        F.concat(F.lit("MERCH-"), (F.col("id") % 5000 + 1).cast("string")).alias("merchant_id"),
        F.concat(F.lit("BR-"), (F.col("id") % 50 + 1).cast("string")).alias("branch_id"),
        F.when(F.col("id") % 1000 == 0, F.lit(True)).otherwise(F.lit(False)).alias("is_flagged"),
        F.when(F.col("id") % 5 == 0,
               F.concat(F.lit("CUST-"), F.lpad(((F.col("id") + 7777) % 50_000 + 1).cast("string"), 6, "0"))
        ).otherwise(F.lit(None)).alias("counterparty_customer_id"),
        F.lit(RUN_ID).alias("_ingestion_run_id"),
        F.current_timestamp().alias("_ingestion_timestamp"),
    )
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze.bronze_transactions")
    cnt = df.count()
    log_end("bronze_transactions", cnt)
    log.info(f"bronze_transactions: {cnt:,} rows")
except Exception as e:
    log_end("bronze_transactions", 0, "FAILED", str(e)); raise

# COMMAND ----------
# -- 5: Card transactions (200 000 rows)
# Columns aligned to silver: txn_amount, currency_code, txn_date, txn_ts, card_number, masked_pan

log_start("bronze_card_transactions")
try:
    df = spark.range(1, 200_001).select(
        F.concat(F.lit("CARD-"), F.lpad(F.col("id").cast("string"), 9, "0")).alias("card_txn_id"),
        F.concat(F.lit("ACC-"), F.lpad((F.col("id") % 80_000 + 1).cast("string"), 8, "0")).alias("account_id"),
        F.concat(F.lit("CUST-"), F.lpad(((F.col("id") % 80_000 + 1) % 50_000 + 1).cast("string"), 6, "0")).alias("customer_id"),
        F.concat(F.lit("4"), F.lpad((F.col("id") * 7919 % 1_000_000_000_000_000).cast("string"), 15, "0")).alias("card_number"),
        F.concat(F.lit("4***"), F.lpad((F.col("id") % 10000).cast("string"), 4, "0")).alias("masked_pan"),
        F.round((F.col("id") * 47.11 % 20_000) + 1, 2).alias("txn_amount"),
        F.lit("ZAR").alias("currency_code"),
        F.when(F.col("id") % 3 == 0, "CONTACTLESS")
         .when(F.col("id") % 2 == 0, "CHIP_PIN")
         .otherwise("ONLINE").alias("transaction_channel"),
        F.concat(F.lit("MERCH-"), (F.col("id") % 5000 + 1).cast("string")).alias("merchant_id"),
        F.array(F.lit("5411"), F.lit("5812"), F.lit("5912"), F.lit("4111"), F.lit("7011"))
          .getItem((F.col("id") % 5).cast("int")).alias("mcc_code"),
        F.to_date(F.timestamp_seconds(
            (F.unix_timestamp(F.current_timestamp()) - F.col("id") % (180 * 24 * 3600)).cast("long")
        )).alias("txn_date"),
        F.timestamp_seconds(
            (F.unix_timestamp(F.current_timestamp()) - F.col("id") % (180 * 24 * 3600)).cast("long")
        ).alias("txn_ts"),
        F.when(F.col("id") % 500 == 0, "DECLINED")
         .when(F.col("id") % 100 == 0, "REVERSED")
         .otherwise("APPROVED").alias("status"),
        F.when(F.col("id") % 500 == 0, F.lit(True)).otherwise(F.lit(False)).alias("is_fraud_flagged"),
        F.lit(RUN_ID).alias("_ingestion_run_id"),
        F.current_timestamp().alias("_ingestion_timestamp"),
    )
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze.bronze_card_transactions")
    cnt = df.count()
    log_end("bronze_card_transactions", cnt)
    log.info(f"bronze_card_transactions: {cnt:,} rows")
except Exception as e:
    log_end("bronze_card_transactions", 0, "FAILED", str(e)); raise

# COMMAND ----------
# -- 6: Loans (30 000 rows)
# Columns aligned to silver: origination_date, interest_rate, currency_code

log_start("bronze_loans")
try:
    df = spark.range(1, 30_001).select(
        F.concat(F.lit("LOAN-"), F.lpad(F.col("id").cast("string"), 7, "0")).alias("loan_id"),
        F.concat(F.lit("CUST-"), F.lpad((F.col("id") % 50_000 + 1).cast("string"), 6, "0")).alias("customer_id"),
        F.concat(F.lit("ACC-"), F.lpad((F.col("id") % 80_000 + 1).cast("string"), 8, "0")).alias("account_id"),
        F.when(F.col("id") % 4 == 0, "HOME_LOAN")
         .when(F.col("id") % 3 == 0, "PERSONAL_LOAN")
         .when(F.col("id") % 2 == 0, "VEHICLE_FINANCE")
         .otherwise("BUSINESS_LOAN").alias("loan_type"),
        F.round((F.col("id") * 3731.17 % 5_000_000) + 10_000, 2).alias("original_amount"),
        F.round((F.col("id") * 1973.31 % 5_000_000) + 5_000,  2).alias("outstanding_balance"),
        F.lit("ZAR").alias("currency_code"),
        F.round((F.col("id") % 200 + 50) / 10.0, 2).alias("interest_rate"),
        (F.col("id") % 360 + 12).alias("term_months"),
        F.date_sub(F.current_date(), (F.col("id") % 1825).cast("int")).alias("origination_date"),
        F.date_add(F.current_date(), (F.col("id") % 3600).cast("int")).alias("maturity_date"),
        F.when(F.col("id") % 10 == 0, "NON_PERFORMING")
         .when(F.col("id") % 5  == 0, "WATCH_LIST")
         .when(F.col("id") % 3  == 0, "SUBSTANDARD")
         .otherwise("PERFORMING").alias("loan_status"),
        F.when(F.col("id") % 10 == 0, (F.col("id") % 270 + 90).cast("int"))
         .otherwise(F.lit(0)).alias("days_past_due"),
        F.lit(RUN_ID).alias("_ingestion_run_id"),
        F.current_timestamp().alias("_ingestion_timestamp"),
    )
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze.bronze_loans")
    cnt = df.count()
    log_end("bronze_loans", cnt)
    log.info(f"bronze_loans: {cnt:,} rows")
except Exception as e:
    log_end("bronze_loans", 0, "FAILED", str(e)); raise

# COMMAND ----------
# -- 7: Credit bureau (50 000 rows)

log_start("bronze_credit_bureau")
try:
    df = spark.range(1, 50_001).select(
        F.concat(F.lit("CUST-"), F.lpad(F.col("id").cast("string"), 6, "0")).alias("customer_id"),
        F.current_date().alias("date"),
        (F.col("id") % 750 + 300).cast("int").alias("credit_score"),
        (F.col("id") % 10).alias("num_active_accounts"),
        F.round((F.col("id") * 1123.77 % 1_000_000), 2).alias("total_exposure"),
        (F.col("id") % 5).alias("num_defaults_last_5yr"),
        F.when(F.col("id") % 4 == 0, "POOR")
         .when(F.col("id") % 3 == 0, "FAIR")
         .when(F.col("id") % 2 == 0, "GOOD")
         .otherwise("EXCELLENT").alias("risk_grade"),
        F.lit(RUN_ID).alias("_ingestion_run_id"),
        F.current_timestamp().alias("_ingestion_timestamp"),
    )
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze.bronze_credit_bureau")
    cnt = df.count()
    log_end("bronze_credit_bureau", cnt)
    log.info(f"bronze_credit_bureau: {cnt:,} rows")
except Exception as e:
    log_end("bronze_credit_bureau", 0, "FAILED", str(e)); raise

# COMMAND ----------
# -- 8: Forex rates (small reference table silver needs for currency conversion)

log_start("bronze_forex_rates")
try:
    forex_data = [
        ("USD/ZAR", 18.5, "2026-06-25"), ("EUR/ZAR", 20.1, "2026-06-25"),
        ("GBP/ZAR", 23.4, "2026-06-25"), ("JPY/ZAR", 0.12, "2026-06-25"),
        ("CNY/ZAR", 2.55, "2026-06-25"), ("AUD/ZAR", 11.8, "2026-06-25"),
    ]
    df = spark.createDataFrame(forex_data, ["currency_pair", "rate", "date"])
    df = df.withColumn("date", F.col("date").cast("date")) \
           .withColumn("_ingestion_run_id", F.lit(RUN_ID)) \
           .withColumn("_ingestion_timestamp", F.current_timestamp())
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze.bronze_forex_rates")
    log_end("bronze_forex_rates", df.count())
    log.info("bronze_forex_rates: 6 rows")
except Exception as e:
    log_end("bronze_forex_rates", 0, "FAILED", str(e)); raise

# COMMAND ----------
# -- 9: Summary

summary = spark.sql(f"""
    SELECT table_name, run_status, rows_ingested, started_at, completed_at
    FROM bronze.pipeline_control
    WHERE run_id = '{RUN_ID}'
    ORDER BY started_at
""")
display(summary)

total_rows = sum(r.rows_ingested or 0 for r in summary.collect())
print(f"\n01_bronze_ingestion -- COMPLETE at {datetime.utcnow().isoformat()}Z")
print(f"Total rows ingested: {total_rows:,}")
