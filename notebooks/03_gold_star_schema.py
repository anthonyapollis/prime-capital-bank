# Databricks notebook source
# Prime Capital Bank — Gold Star Schema
# Notebook: 03_gold_star_schema.py
# Author:   Prime Capital Bank — Data Engineering
# Purpose:  Build gold-layer dimensions (SCD Type 2) and fact tables
#           with incremental MERGE, Z-ordering, OPTIMIZE and VACUUM.


# COMMAND ----------

import uuid, logging
from datetime import datetime
from pyspark.sql import functions as F
from delta.tables import DeltaTable

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("gold_star_schema")
BRONZE   = "bronze"
SILVER   = "silver"
GOLD     = "gold"
RUN_ID   = str(uuid.uuid4())
HIGH_WATERMARK_TABLE = f"`{GOLD}`.`etl_watermarks`"

# ── Helper: watermark management ──────────────────────────────────────────────

def get_watermark(table_name: str) -> str:
    """Return the last successful load timestamp for a gold table."""
    try:
        result = spark.sql(f"""
            SELECT last_loaded_at FROM {HIGH_WATERMARK_TABLE}
            WHERE table_name = '{table_name}'
        """).collect()
        return result[0][0] if result else "1900-01-01 00:00:00"
    except Exception:
        return "1900-01-01 00:00:00"


def set_watermark(table_name: str):
    spark.sql(f"""
        MERGE INTO {HIGH_WATERMARK_TABLE} AS t
        USING (SELECT '{table_name}' AS table_name, current_timestamp() AS last_loaded_at) AS s
        ON t.table_name = s.table_name
        WHEN MATCHED THEN UPDATE SET t.last_loaded_at = s.last_loaded_at
        WHEN NOT MATCHED THEN INSERT (table_name, last_loaded_at) VALUES (s.table_name, s.last_loaded_at)
    """)

# COMMAND ----------

# -- ## 0 — Bootstrap Gold Tables

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS `{GOLD}`.`etl_watermarks` (
        table_name    STRING,
        last_loaded_at TIMESTAMP
    )
    USING DELTA
""")

# COMMAND ----------

# -- ## 1 — dim_customer (SCD Type 2)

# COMMAND ----------

def build_dim_customer():
    TABLE = f"`{GOLD}`.`dim_customer`"

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            customer_sk          BIGINT GENERATED ALWAYS AS IDENTITY,
            customer_id          STRING          NOT NULL,
            first_name           STRING,
            last_name            STRING,
            date_of_birth        DATE,
            id_number            STRING          COMMENT 'Masked in non-prod',
            email_address        STRING          COMMENT 'Masked in non-prod',
            mobile_number        STRING          COMMENT 'Masked in non-prod',
            street_address       STRING,
            suburb               STRING,
            city                 STRING,
            province             STRING,
            postal_code          STRING,
            country_code         STRING,
            risk_segment         STRING,
            customer_status      STRING,
            age_years            INTEGER,
            onboarding_date      DATE,
            -- SCD Type 2 columns
            scd_effective_date   DATE            NOT NULL,
            scd_expiry_date      DATE,
            scd_is_current       BOOLEAN         NOT NULL,
            _silver_run_id       STRING,
            _gold_loaded_at      TIMESTAMP
        )
        USING DELTA
        COMMENT 'SCD Type 2 customer dimension'
        TBLPROPERTIES (
            'delta.enableChangeDataFeed' = 'true',
            'delta.autoOptimize.optimizeWrite' = 'true'
        )
    """)

    src = spark.table(f"`{SILVER}`.`silver_customers`") \
               .withColumn("scd_effective_date", F.current_date()) \
               .withColumn("scd_expiry_date",    F.lit(None).cast("date")) \
               .withColumn("scd_is_current",     F.lit(True)) \
               .withColumn("_gold_loaded_at",    F.current_timestamp())

    try:
        target = DeltaTable.forName(spark, TABLE)
        (
            target.alias("t")
                  .merge(src.alias("s"), "t.customer_id = s.customer_id AND t.scd_is_current = true")
                  # Close old record if any tracked attribute changed
                  .whenMatchedUpdate(
                      condition="""
                          t.risk_segment   <> s.risk_segment   OR
                          t.customer_status <> s.customer_status OR
                          t.mobile_number  <> s.mobile_number  OR
                          t.email_address  <> s.email_address  OR
                          t.street_address <> s.street_address
                      """,
                      set={
                          "scd_expiry_date": "current_date() - INTERVAL 1 DAY",
                          "scd_is_current":  "false",
                      }
                  )
                  .whenNotMatchedInsertAll()
                  .execute()
        )
        # Insert new current record for changed rows
        changed = src.join(
            spark.table(TABLE).filter("scd_is_current = false AND scd_expiry_date = current_date() - 1"),
            "customer_id", "inner"
        ).select(src.columns)
        if changed.count() > 0:
            changed.write.format("delta").mode("append").saveAsTable(TABLE)
    except Exception as e:
        log.warning(f"dim_customer merge error, falling back to overwrite: {e}")
        src.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TABLE)

    set_watermark("dim_customer")
    log.info(f"dim_customer — built")

build_dim_customer()

# COMMAND ----------

# -- ## 2 — dim_account (SCD Type 2)

# COMMAND ----------

def build_dim_account():
    TABLE = f"`{GOLD}`.`dim_account`"

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            account_sk           BIGINT GENERATED ALWAYS AS IDENTITY,
            account_id           STRING NOT NULL,
            customer_id          STRING,
            account_type         STRING,
            account_status       STRING,
            product_id           STRING,
            branch_id            STRING,
            open_date            DATE,
            close_date           DATE,
            current_balance      DOUBLE,
            available_balance    DOUBLE,
            currency_code        STRING,
            days_open            INTEGER,
            scd_effective_date   DATE NOT NULL,
            scd_expiry_date      DATE,
            scd_is_current       BOOLEAN NOT NULL,
            _gold_loaded_at      TIMESTAMP
        )
        USING DELTA
        TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
    """)

    src = spark.table(f"`{SILVER}`.`silver_accounts`") \
               .withColumn("scd_effective_date", F.current_date()) \
               .withColumn("scd_expiry_date",    F.lit(None).cast("date")) \
               .withColumn("scd_is_current",     F.lit(True)) \
               .withColumn("_gold_loaded_at",    F.current_timestamp())

    try:
        target = DeltaTable.forName(spark, TABLE)
        (
            target.alias("t")
                  .merge(src.alias("s"), "t.account_id = s.account_id AND t.scd_is_current = true")
                  .whenMatchedUpdate(
                      condition="t.account_status <> s.account_status OR t.current_balance <> s.current_balance",
                      set={"scd_expiry_date": "current_date() - INTERVAL 1 DAY", "scd_is_current": "false"}
                  )
                  .whenNotMatchedInsertAll()
                  .execute()
        )
    except Exception as e:
        log.warning(f"dim_account merge fallback: {e}")
        src.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TABLE)

    set_watermark("dim_account")
    log.info("dim_account — built")

build_dim_account()

# COMMAND ----------

# -- ## 3 — dim_date

# COMMAND ----------

def build_dim_date():
    TABLE = f"`{GOLD}`.`dim_date`"
    existing = spark.catalog.tableExists(TABLE)
    if existing:
        log.info("dim_date already exists — skipping rebuild")
        return

    from pyspark.sql.types import StructType, StructField, DateType
    dates_df = spark.sql("""
        SELECT explode(sequence(
            to_date('2000-01-01'), to_date('2035-12-31'), interval 1 day
        )) AS full_date
    """)

    sa_public_holidays = [
        "2024-01-01","2024-03-21","2024-03-29","2024-04-01","2024-04-27",
        "2024-05-01","2024-06-16","2024-08-09","2024-09-24","2024-12-16",
        "2024-12-25","2024-12-26",
    ]
    holidays_df = spark.createDataFrame(
        [(h,) for h in sa_public_holidays], ["holiday_date"]
    ).withColumn("holiday_date", F.col("holiday_date").cast("date"))

    dim_date = (
        dates_df
        .withColumn("date_id",          F.date_format("full_date", "yyyyMMdd").cast("integer"))
        .withColumn("year",             F.year("full_date"))
        .withColumn("quarter",          F.quarter("full_date"))
        .withColumn("month_num",        F.month("full_date"))
        .withColumn("month_name",       F.date_format("full_date", "MMMM"))
        .withColumn("month_short",      F.date_format("full_date", "MMM"))
        .withColumn("week_of_year",     F.weekofyear("full_date"))
        .withColumn("day_of_month",     F.dayofmonth("full_date"))
        .withColumn("day_of_week_num",  F.dayofweek("full_date"))
        .withColumn("day_of_week_name", F.date_format("full_date", "EEEE"))
        .withColumn("is_weekend",       F.dayofweek("full_date").isin(1, 7).cast("boolean"))
        .withColumn("fiscal_year",
                    F.when(F.month("full_date") >= 4, F.year("full_date"))
                     .otherwise(F.year("full_date") - 1))
        .withColumn("fiscal_quarter",
                    F.when(F.month("full_date").isin(4,5,6),   F.lit(1))
                     .when(F.month("full_date").isin(7,8,9),   F.lit(2))
                     .when(F.month("full_date").isin(10,11,12), F.lit(3))
                     .otherwise(F.lit(4)))
        .join(holidays_df, dates_df["full_date"] == holidays_df["holiday_date"], "left")
        .withColumn("is_public_holiday", F.col("holiday_date").isNotNull().cast("boolean"))
        .withColumn("is_business_day",
                    (~F.col("is_weekend") & ~F.col("is_public_holiday")).cast("boolean"))
        .drop("holiday_date")
    )

    dim_date.write.format("delta").mode("overwrite").saveAsTable(TABLE)
    log.info(f"dim_date — {dim_date.count():,} rows built")

build_dim_date()

# COMMAND ----------

# -- ## 4 — dim_product (SCD Type 1 — simple overwrite)

# COMMAND ----------

def build_dim_product():
    TABLE = f"`{GOLD}`.`dim_product`"
    from pyspark.sql import Row
    import datetime as _dt
    _products = [
        ("PROD_001","HOME_LOAN",       "Retail Mortgage",       "Mortgage",         0.35, 9.8,  "Retail"),
        ("PROD_002","PERSONAL_LOAN",   "Personal Loan",         "Unsecured Retail", 1.00, 22.4, "Retail"),
        ("PROD_003","VEHICLE_FINANCE", "Vehicle Finance",       "Secured Retail",   0.75, 11.2, "Retail"),
        ("PROD_004","BUSINESS_LOAN",   "Business Loan",         "Corporate",        0.85, 12.6, "Corporate"),
        ("PROD_005","CREDIT_CARD",     "Credit Card",           "Revolving Credit", 1.50, 24.8, "Retail"),
        ("PROD_006","OVERDRAFT",       "Overdraft Facility",    "Unsecured Retail", 1.00, 18.5, "Retail"),
        ("PROD_007","FIXED_DEPOSIT",   "Fixed Deposit",         "Deposit",          0.00, 7.2,  "Retail"),
        ("PROD_008","CURRENT_ACCOUNT", "Current Account",       "Transaction",      0.00, 0.0,  "Retail"),
        ("PROD_009","SAVINGS_ACCOUNT", "Savings Account",       "Deposit",          0.00, 5.5,  "Retail"),
    ]
    _now = _dt.datetime.now()
    _rows = [Row(product_id=pid, product_code=code, product_name=name, product_family=family,
                 rwa_weight=rwa, avg_interest_rate=rate, customer_segment=seg,
                 is_active=True, _gold_loaded_at=_now)
             for pid, code, name, family, rwa, rate, seg in _products]
    src = spark.createDataFrame(_rows).withColumn("_gold_loaded_at", F.current_timestamp())
    src.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TABLE)
    log.info(f"dim_product — {src.count():,} rows")

build_dim_product()

# COMMAND ----------

# -- ## 5 — fact_transaction (Incremental Append)

# COMMAND ----------

def build_fact_transaction():
    TABLE     = f"`{GOLD}`.`fact_transaction`"
    watermark = get_watermark("fact_transaction")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            transaction_sk    BIGINT GENERATED ALWAYS AS IDENTITY,
            transaction_id    STRING     NOT NULL,
            account_id        STRING,
            customer_id       STRING,
            transaction_date  DATE,
            transaction_ts    TIMESTAMP,
            amount_zar        DOUBLE,
            transaction_type  STRING,
            transaction_status STRING,
            channel           STRING,
            is_reversal       BOOLEAN,
            branch_id         STRING,
            _gold_loaded_at   TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (transaction_date)
        TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
    """)

    new_rows = (
        spark.table(f"`{SILVER}`.`silver_transactions`")
             .filter(F.col("_silver_loaded_at") > F.lit(watermark))
             .withColumn("_gold_loaded_at", F.current_timestamp())
             .select("transaction_id","account_id","customer_id","transaction_date",
                     "transaction_ts","amount_zar","transaction_type","transaction_status",
                     "channel","is_reversal","branch_id","_gold_loaded_at")
    )

    count = new_rows.count()
    if count > 0:
        new_rows.write.format("delta").mode("append").saveAsTable(TABLE)
        log.info(f"fact_transaction — {count:,} new rows appended")
    else:
        log.info("fact_transaction — no new rows since watermark")

    set_watermark("fact_transaction")

build_fact_transaction()

# COMMAND ----------

# -- ## 6 — fact_loan_portfolio (Upsert via MERGE)

# COMMAND ----------

def build_fact_loan_portfolio():
    TABLE = f"`{GOLD}`.`fact_loan_portfolio`"

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            loan_sk               BIGINT GENERATED ALWAYS AS IDENTITY,
            loan_id               STRING     NOT NULL,
            customer_id           STRING,
            account_id            STRING,
            loan_type             STRING,
            loan_status           STRING,
            origination_date      DATE,
            maturity_date         DATE,
            original_amount_zar   DOUBLE,
            outstanding_balance_zar DOUBLE,
            interest_rate         DOUBLE,
            days_past_due         INTEGER,
            is_npl                BOOLEAN,
            loan_term_months      INTEGER,
            pd_score              DOUBLE     COMMENT 'Populated by 04_ml_credit_scoring',
            lgd_score             DOUBLE     COMMENT 'Populated by 04_ml_credit_scoring',
            ecl_amount_zar        DOUBLE     COMMENT 'Expected Credit Loss = PD x LGD x EAD',
            _gold_loaded_at       TIMESTAMP
        )
        USING DELTA
        TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
    """)

    src = (
        spark.table(f"`{SILVER}`.`silver_loans`")
             .withColumn("_gold_loaded_at", F.current_timestamp())
             .withColumn("pd_score",        F.lit(None).cast("double"))
             .withColumn("lgd_score",       F.lit(None).cast("double"))
             .withColumn("ecl_amount_zar",  F.lit(None).cast("double"))
    )

    try:
        target = DeltaTable.forName(spark, TABLE)
        (
            target.alias("t")
                  .merge(src.alias("s"), "t.loan_id = s.loan_id")
                  .whenMatchedUpdate(set={
                      "loan_status":              "s.loan_status",
                      "outstanding_balance_zar":  "s.outstanding_balance_zar",
                      "days_past_due":             "s.days_past_due",
                      "is_npl":                    "s.is_npl",
                      "_gold_loaded_at":           "s._gold_loaded_at",
                  })
                  .whenNotMatchedInsertAll()
                  .execute()
        )
    except Exception as e:
        log.warning(f"fact_loan_portfolio merge fallback: {e}")
        src.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TABLE)

    set_watermark("fact_loan_portfolio")
    log.info("fact_loan_portfolio — built")

build_fact_loan_portfolio()

# COMMAND ----------

# -- ## 7 — fact_fraud_event (placeholder — populated by 05_fraud_detection)

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS `{GOLD}`.`fact_fraud_event` (
        fraud_event_sk      BIGINT GENERATED ALWAYS AS IDENTITY,
        card_txn_id         STRING NOT NULL,
        customer_id         STRING,
        account_id          STRING,
        txn_date            DATE,
        txn_amount_zar      DOUBLE,
        fraud_score         DOUBLE,
        fraud_label         STRING    COMMENT 'CONFIRMED | SUSPECTED | CLEARED',
        detection_model     STRING,
        alert_raised_at     TIMESTAMP,
        resolved_at         TIMESTAMP,
        _gold_loaded_at     TIMESTAMP
    )
    USING DELTA
    PARTITIONED BY (txn_date)
    TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
""")
log.info("fact_fraud_event DDL ready")

# COMMAND ----------

# -- ## 8 — fact_aml_case (placeholder — populated by 06_aml_risk_scoring)

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS `{GOLD}`.`fact_aml_case` (
        aml_case_sk         BIGINT GENERATED ALWAYS AS IDENTITY,
        case_id             STRING NOT NULL,
        customer_id         STRING,
        case_type           STRING  COMMENT 'STRUCTURING | LAYERING | SMURFING | OTHER',
        risk_score          DOUBLE,
        sar_candidate       BOOLEAN,
        detected_at         TIMESTAMP,
        reviewed_at         TIMESTAMP,
        case_status         STRING,
        assigned_analyst    STRING,
        _gold_loaded_at     TIMESTAMP
    )
    USING DELTA
    TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
""")
log.info("fact_aml_case DDL ready")

# COMMAND ----------

# -- ## 9 — Z-Order and OPTIMIZE

# COMMAND ----------

OPTIMIZE_TARGETS = [
    ("fact_transaction",    "customer_id, account_id, transaction_date"),
    ("fact_loan_portfolio", "customer_id, loan_id"),
    ("fact_fraud_event",    "customer_id, txn_date"),
    ("fact_aml_case",       "customer_id"),
    ("dim_customer",        "customer_id"),
    ("dim_account",         "account_id, customer_id"),
]

for table_name, zorder_cols in OPTIMIZE_TARGETS:
    try:
        log.info(f"OPTIMIZE {table_name} ZORDER BY ({zorder_cols})")
        spark.sql(f"""
            OPTIMIZE `{GOLD}`.`{table_name}`
            ZORDER BY ({zorder_cols})
        """)
    except Exception as e:
        log.warning(f"OPTIMIZE failed for {table_name}: {e}")

# COMMAND ----------

# -- ## 10 — VACUUM (retain 7 days)

# COMMAND ----------

ALL_GOLD_TABLES = [
    "dim_customer","dim_account","dim_date","dim_product",
    "fact_transaction","fact_loan_portfolio","fact_fraud_event","fact_aml_case",
    "etl_watermarks",
]

for table_name in ALL_GOLD_TABLES:
    try:
        spark.sql(f"VACUUM `{GOLD}`.`{table_name}` RETAIN 168 HOURS")
        log.info(f"VACUUM {table_name} — done")
    except Exception as e:
        log.warning(f"VACUUM failed for {table_name}: {e}")

print(f"\n03_gold_star_schema — COMPLETE  (run_id={RUN_ID})")
