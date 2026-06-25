-- =============================================================================
-- PRIME CAPITAL BANK — GOLD ANALYTICAL VIEWS
-- Unity Catalog / Delta Lake — Databricks SQL
-- Layer   : Gold (presentation views over star schema)
-- Author  : Data Engineering Team
-- Created : 2026-06-24
-- Notes   : All views are built on the gold star schema (dim_* and fact_*
--           tables). Views are designed to be queryable by Power BI, Tableau,
--           and ad-hoc SQL users. No aggregation state is stored in these views
--           unless materialised separately; all logic is pure SQL.
--           Each view is DROP IF EXISTS + CREATE OR REPLACE for idempotency.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- VIEW 1: vw_customer_360
--    Full 360-degree customer view joining all major facts.
--    Grain: one row per current customer (latest snapshot of each fact).
--    Used by: RM workbench, customer analytics, CRM enrichment.
-- ---------------------------------------------------------------------------
DROP VIEW IF EXISTS prime_capital.gold.vw_customer_360;

CREATE OR REPLACE VIEW prime_capital.gold.vw_customer_360
COMMENT 'Customer 360 view. One row per active customer, combining demographics, product holdings, total balances, lending exposure, card utilisation, payment activity, and fraud/AML risk flags. Used by relationship managers and customer analytics.'
AS
WITH customer_base AS (
  -- Latest version of each customer (is_current = true)
  SELECT
    c.customer_sk,
    c.customer_id,
    c.full_name,
    c.first_name,
    c.last_name,
    c.date_of_birth,
    c.age_years,
    c.age_band,
    c.gender,
    c.residential_province_name,
    c.residential_city,
    c.employment_status,
    c.gross_monthly_income,
    c.annual_income,
    c.income_band,
    c.customer_type,
    c.customer_segment,
    c.customer_status,
    c.risk_segment,
    c.clv_tier,
    c.fica_status,
    c.pep_flag,
    c.sanctions_flag,
    c.onboarding_date,
    c.tenure_years,
    c.tenure_band,
    c.marketing_consent
  FROM prime_capital.gold.dim_customer c
  WHERE c.is_current = true
    AND c.customer_status IN ('ACTIVE', 'DORMANT')
),

-- Total account balances and count
account_summary AS (
  SELECT
    c.customer_id,
    COUNT(DISTINCT a.account_id)                          AS total_accounts,
    SUM(fab.closing_balance)                              AS total_deposit_balance,
    SUM(CASE WHEN a.account_type_code = 'CURRENT'
          THEN fab.closing_balance ELSE 0 END)            AS current_account_balance,
    SUM(CASE WHEN a.account_type_code = 'SAVINGS'
          THEN fab.closing_balance ELSE 0 END)            AS savings_balance,
    SUM(CASE WHEN a.account_type_code = 'FIXED'
          THEN fab.closing_balance ELSE 0 END)            AS fixed_deposit_balance,
    MAX(fab.snapshot_date)                                AS balance_as_at_date
  FROM prime_capital.gold.fact_account_balance fab
  JOIN prime_capital.gold.dim_account a
    ON fab.account_sk = a.account_sk AND a.is_current = true
  JOIN prime_capital.gold.dim_customer c
    ON a.customer_id = c.customer_id AND c.is_current = true
  -- Last 1 day of snapshots only
  WHERE fab.snapshot_date = (SELECT MAX(snapshot_date) FROM prime_capital.gold.fact_account_balance)
  GROUP BY c.customer_id
),

-- Lending exposure summary (latest snapshot)
loan_summary AS (
  SELECT
    c.customer_id,
    COUNT(DISTINCT flp.loan_id)                                        AS active_loan_count,
    SUM(flp.outstanding_balance)                                       AS total_loan_balance,
    SUM(flp.arrears_amount)                                            AS total_arrears,
    MAX(flp.delinquency_days)                                          AS max_delinquency_days,
    SUM(flp.provision_amount)                                          AS total_provision,
    CAST(MAX(CASE WHEN flp.npl_flag = true THEN 1 ELSE 0 END) AS BOOLEAN) AS has_npl
  FROM prime_capital.gold.fact_loan_portfolio flp
  JOIN prime_capital.gold.dim_customer c
    ON flp.customer_sk = c.customer_sk AND c.is_current = true
  WHERE flp.snapshot_date = (SELECT MAX(snapshot_date) FROM prime_capital.gold.fact_loan_portfolio)
    AND flp.loan_status NOT IN ('SETTLED', 'WRITTEN_OFF')
  GROUP BY c.customer_id
),

-- Card utilisation (latest snapshot)
card_summary AS (
  SELECT
    c.customer_id,
    COUNT(DISTINCT a.account_id)                           AS card_count,
    SUM(sc.credit_limit)                                   AS total_credit_limit,
    SUM(sc.current_balance)                                AS total_card_balance,
    AVG(sc.utilisation_rate)                               AS avg_card_utilisation,
    MAX(sc.reward_points_balance)                          AS max_reward_points,
    CAST(MAX(CASE WHEN sc.overlimit_flag = true THEN 1 ELSE 0 END) AS BOOLEAN) AS has_overlimit_card
  FROM prime_capital.silver.silver_credit_cards sc
  JOIN prime_capital.gold.dim_account a
    ON sc.card_id = a.account_id AND a.is_current = true
  JOIN prime_capital.gold.dim_customer c
    ON sc.customer_id = c.customer_id AND c.is_current = true
  WHERE sc.snapshot_date = (SELECT MAX(snapshot_date) FROM prime_capital.silver.silver_credit_cards)
    AND sc.card_status = 'ACTIVE'
  GROUP BY c.customer_id
),

-- Recent transaction activity (last 90 days)
transaction_activity AS (
  SELECT
    c.customer_id,
    COUNT(ft.transaction_sk)                               AS tx_count_90d,
    SUM(CASE WHEN ft.is_debit THEN ft.amount_zar ELSE 0 END) AS total_spend_90d,
    SUM(CASE WHEN NOT ft.is_debit THEN ft.amount_zar ELSE 0 END) AS total_credits_90d,
    COUNT(DISTINCT ft.channel_sk)                          AS distinct_channels_used,
    CAST(MAX(CASE WHEN ft.is_digital THEN 1 ELSE 0 END) AS BOOLEAN) AS uses_digital_channels,
    MAX(ft.transaction_date)                               AS last_transaction_date
  FROM prime_capital.gold.fact_transaction ft
  JOIN prime_capital.gold.dim_customer c
    ON ft.customer_sk = c.customer_sk AND c.is_current = true
  WHERE ft.transaction_date >= DATEADD(DAY, -90, CURRENT_DATE)
  GROUP BY c.customer_id
),

-- AML and fraud risk flags
risk_flags AS (
  SELECT
    c.customer_id,
    COUNT(fac.aml_case_sk)                                AS open_aml_cases,
    CAST(MAX(CASE WHEN fac.sar_filed_flag = true THEN 1 ELSE 0 END) AS BOOLEAN) AS has_sar_filed,
    COUNT(ffe.fraud_sk)                                   AS fraud_events_12m,
    SUM(ffe.net_loss_amount)                              AS total_fraud_loss_12m
  FROM prime_capital.gold.dim_customer c
  LEFT JOIN prime_capital.gold.fact_aml_case fac
    ON c.customer_sk = fac.customer_sk
    AND fac.case_status = 'OPEN'
  LEFT JOIN prime_capital.gold.fact_fraud_event ffe
    ON c.customer_sk = ffe.customer_sk
    AND ffe.alert_date >= DATEADD(DAY, -365, CURRENT_DATE)
  WHERE c.is_current = true
  GROUP BY c.customer_id
)

SELECT
  -- Customer core
  cb.customer_sk,
  cb.customer_id,
  cb.full_name,
  cb.first_name,
  cb.last_name,
  cb.date_of_birth,
  cb.age_years,
  cb.age_band,
  cb.gender,
  cb.residential_province_name,
  cb.residential_city,
  cb.employment_status,
  cb.gross_monthly_income,
  cb.annual_income,
  cb.income_band,
  cb.customer_type,
  cb.customer_segment,
  cb.customer_status,
  cb.risk_segment,
  cb.clv_tier,
  cb.fica_status,
  cb.pep_flag,
  cb.sanctions_flag,
  cb.onboarding_date,
  cb.tenure_years,
  cb.tenure_band,
  cb.marketing_consent,
  -- Account summary
  COALESCE(accs.total_accounts, 0)            AS total_accounts,
  COALESCE(accs.total_deposit_balance, 0)     AS total_deposit_balance,
  COALESCE(accs.current_account_balance, 0)   AS current_account_balance,
  COALESCE(accs.savings_balance, 0)           AS savings_balance,
  COALESCE(accs.fixed_deposit_balance, 0)     AS fixed_deposit_balance,
  accs.balance_as_at_date,
  -- Lending
  COALESCE(ls.active_loan_count, 0)           AS active_loan_count,
  COALESCE(ls.total_loan_balance, 0)          AS total_loan_balance,
  COALESCE(ls.total_arrears, 0)               AS total_arrears,
  COALESCE(ls.max_delinquency_days, 0)        AS max_delinquency_days,
  COALESCE(ls.total_provision, 0)             AS total_provision,
  COALESCE(ls.has_npl, false)                 AS has_npl,
  -- Cards
  COALESCE(cs.card_count, 0)                  AS active_card_count,
  COALESCE(cs.total_credit_limit, 0)          AS total_credit_limit,
  COALESCE(cs.total_card_balance, 0)          AS total_card_balance,
  ROUND(COALESCE(cs.avg_card_utilisation, 0), 2) AS avg_card_utilisation_pct,
  COALESCE(cs.max_reward_points, 0)           AS reward_points_balance,
  COALESCE(cs.has_overlimit_card, false)       AS has_overlimit_card,
  -- Total exposure (deposit - lending = net position)
  COALESCE(accs.total_deposit_balance, 0)
    - COALESCE(ls.total_loan_balance, 0)
    - COALESCE(cs.total_card_balance, 0)      AS net_banking_position,
  -- Transaction activity (last 90 days)
  COALESCE(ta.tx_count_90d, 0)               AS tx_count_last_90d,
  COALESCE(ta.total_spend_90d, 0)            AS total_spend_last_90d,
  COALESCE(ta.total_credits_90d, 0)          AS total_credits_last_90d,
  ta.last_transaction_date,
  COALESCE(ta.distinct_channels_used, 0)     AS channels_used_last_90d,
  COALESCE(ta.uses_digital_channels, false)  AS is_digital_user,
  -- Risk flags
  COALESCE(rf.open_aml_cases, 0)             AS open_aml_cases,
  COALESCE(rf.has_sar_filed, false)           AS has_sar_filed,
  COALESCE(rf.fraud_events_12m, 0)            AS fraud_events_last_12m,
  COALESCE(rf.total_fraud_loss_12m, 0)        AS total_fraud_loss_last_12m,
  -- Derived risk indicator
  CASE
    WHEN cb.pep_flag = true OR cb.sanctions_flag = true  THEN 'VERY_HIGH'
    WHEN COALESCE(rf.open_aml_cases, 0) > 0              THEN 'HIGH'
    WHEN COALESCE(ls.has_npl, false) = true               THEN 'HIGH'
    WHEN cb.risk_segment = 'ELEVATED'                     THEN 'MEDIUM'
    ELSE 'STANDARD'
  END AS composite_risk_indicator,
  CURRENT_TIMESTAMP AS view_generated_at
FROM customer_base cb
LEFT JOIN account_summary    accs ON cb.customer_id = accs.customer_id
LEFT JOIN loan_summary        ls  ON cb.customer_id = ls.customer_id
LEFT JOIN card_summary        cs  ON cb.customer_id = cs.customer_id
LEFT JOIN transaction_activity ta ON cb.customer_id = ta.customer_id
LEFT JOIN risk_flags           rf ON cb.customer_id = rf.customer_id;


-- ---------------------------------------------------------------------------
-- VIEW 2: vw_npl_portfolio
--    Non-performing loan portfolio with provision coverage analysis.
--    Grain: one row per NPL loan (latest snapshot).
--    Used by: Credit Risk, IFRS 9 reporting, SARB returns.
-- ---------------------------------------------------------------------------
DROP VIEW IF EXISTS prime_capital.gold.vw_npl_portfolio;

CREATE OR REPLACE VIEW prime_capital.gold.vw_npl_portfolio
COMMENT 'Non-performing loan portfolio view. Shows all loans classified as NPL (>=90 days past due or ECL Stage 3), with outstanding balance, provision amount, coverage ratio, LTV, and write-off readiness assessment. Used for IFRS 9 regulatory reporting and credit risk dashboards.'
AS
SELECT
  -- Loan identifiers
  flp.loan_id,
  -- Customer
  c.customer_id,
  c.full_name                                         AS customer_name,
  c.customer_type,
  c.customer_segment,
  c.risk_segment,
  -- Product and branch
  p.product_name,
  p.product_sub_type                                  AS loan_type,
  b.branch_name,
  b.province_name                                     AS branch_province,
  -- Loan financials
  flp.original_amount,
  flp.outstanding_balance,
  flp.principal_paid,
  ROUND((flp.principal_paid / NULLIF(flp.original_amount, 0)) * 100, 2) AS principal_paid_pct,
  flp.interest_accrued,
  flp.arrears_amount,
  flp.delinquency_days,
  -- Delinquency classification
  CASE
    WHEN flp.delinquency_days BETWEEN 90  AND 120 THEN '90-120 DPD'
    WHEN flp.delinquency_days BETWEEN 121 AND 180 THEN '121-180 DPD'
    WHEN flp.delinquency_days BETWEEN 181 AND 365 THEN '181-365 DPD'
    WHEN flp.delinquency_days > 365               THEN '365+ DPD'
    ELSE '90 DPD'
  END                                                 AS delinquency_band,
  -- IFRS 9 provisioning
  flp.ecl_stage,
  flp.provision_rate,
  flp.provision_amount,
  ROUND((flp.provision_amount / NULLIF(flp.outstanding_balance, 0)) * 100, 2) AS provision_coverage_pct,
  -- Collateral / LTV
  flp.collateral_value,
  flp.ltv_ratio,
  ROUND((flp.outstanding_balance - flp.collateral_value), 2) AS unsecured_exposure,
  CASE WHEN flp.ltv_ratio > 100 THEN true ELSE false END      AS is_underwater,
  -- Write-off assessment
  CASE
    WHEN flp.delinquency_days > 365 AND flp.collateral_value < (flp.outstanding_balance * 0.1)
      THEN 'RECOMMEND_WRITE_OFF'
    WHEN flp.delinquency_days > 180
      THEN 'REVIEW_FOR_WRITE_OFF'
    ELSE 'ACTIVE_RECOVERY'
  END                                                  AS write_off_recommendation,
  -- Legal action
  flp.legal_action_flag,
  flp.restructured_flag,
  -- Snapshot date
  flp.snapshot_date
FROM prime_capital.gold.fact_loan_portfolio flp
JOIN prime_capital.gold.dim_customer c
  ON flp.customer_sk = c.customer_sk AND c.is_current = true
JOIN prime_capital.gold.dim_product p
  ON flp.product_sk = p.product_sk AND p.is_current = true
JOIN prime_capital.gold.dim_branch b
  ON flp.branch_sk = b.branch_sk AND b.is_current = true
WHERE flp.npl_flag = true
  AND flp.loan_status NOT IN ('SETTLED', 'WRITTEN_OFF')
  AND flp.snapshot_date = (SELECT MAX(snapshot_date) FROM prime_capital.gold.fact_loan_portfolio);


-- ---------------------------------------------------------------------------
-- VIEW 3: vw_daily_transaction_summary
--    Daily transaction volumes, values, and channel mix.
--    Grain: one row per channel per transaction_date.
--    Used by: Operations, digital team, channel management.
-- ---------------------------------------------------------------------------
DROP VIEW IF EXISTS prime_capital.gold.vw_daily_transaction_summary;

CREATE OR REPLACE VIEW prime_capital.gold.vw_daily_transaction_summary
COMMENT 'Daily transaction summary aggregated by channel and transaction category. Shows volumes, total values, average transaction size, debit/credit split, and fraud rates per channel per day. Used for operations monitoring, channel management, and digital banking KPIs.'
AS
SELECT
  ft.transaction_date,
  -- Date attributes from dimension
  dd.day_name,
  dd.is_weekend,
  dd.is_public_holiday,
  dd.is_business_day,
  dd.financial_year,
  dd.financial_quarter,
  -- Channel
  ch.channel_code,
  ch.channel_name,
  ch.channel_category,
  ch.is_digital,
  -- Transaction category
  ft.tx_category,
  -- Volume metrics
  COUNT(ft.transaction_sk)                               AS transaction_count,
  COUNT(DISTINCT ft.customer_sk)                         AS unique_customers,
  COUNT(DISTINCT ft.account_sk)                          AS unique_accounts,
  -- Value metrics
  SUM(ft.amount_zar)                                     AS total_amount_zar,
  SUM(CASE WHEN ft.is_debit THEN ft.amount_zar ELSE 0 END) AS total_debits_zar,
  SUM(CASE WHEN NOT ft.is_debit THEN ft.amount_zar ELSE 0 END) AS total_credits_zar,
  ROUND(AVG(ft.amount_zar), 2)                           AS avg_transaction_amount,
  MIN(ft.amount_zar)                                     AS min_transaction_amount,
  MAX(ft.amount_zar)                                     AS max_transaction_amount,
  -- Fee revenue
  SUM(ft.fee_amount_zar)                                 AS total_fee_revenue_zar,
  -- Fraud metrics
  COUNT(CASE WHEN ft.fraud_flag = true THEN 1 END)       AS fraud_flagged_count,
  SUM(CASE WHEN ft.fraud_flag = true THEN ft.amount_zar ELSE 0 END) AS fraud_flagged_amount,
  ROUND(
    COUNT(CASE WHEN ft.fraud_flag = true THEN 1 END) * 100.0
    / NULLIF(COUNT(ft.transaction_sk), 0), 4
  )                                                      AS fraud_rate_pct,
  -- Reversal metrics
  COUNT(CASE WHEN ft.is_reversal = true THEN 1 END)      AS reversal_count,
  SUM(CASE WHEN ft.is_reversal = true THEN ft.amount_zar ELSE 0 END) AS reversal_amount_zar
FROM prime_capital.gold.fact_transaction ft
JOIN prime_capital.gold.dim_date dd
  ON ft.date_sk = dd.date_sk
JOIN prime_capital.gold.dim_channel ch
  ON ft.channel_sk = ch.channel_sk
WHERE ft.is_reversal = false  -- Exclude reversals from primary counts; reported separately
GROUP BY
  ft.transaction_date, dd.day_name, dd.is_weekend, dd.is_public_holiday,
  dd.is_business_day, dd.financial_year, dd.financial_quarter,
  ch.channel_code, ch.channel_name, ch.channel_category, ch.is_digital,
  ft.tx_category;


-- ---------------------------------------------------------------------------
-- VIEW 4: vw_fraud_dashboard
--    Fraud operations dashboard: rates, losses, recovery by type and channel.
--    Grain: one row per fraud_type per alert_date.
--    Used by: Fraud operations, risk management, EXCO.
-- ---------------------------------------------------------------------------
DROP VIEW IF EXISTS prime_capital.gold.vw_fraud_dashboard;

CREATE OR REPLACE VIEW prime_capital.gold.vw_fraud_dashboard
COMMENT 'Fraud dashboard view. Daily fraud metrics by fraud type and channel, including alert count, confirmed fraud rate, gross loss, recovery amount, net loss, and resolution SLA compliance. Covers rolling 12-month window. Used by fraud operations centre and risk EXCO.'
AS
SELECT
  ffe.alert_date,
  dd.financial_year,
  dd.financial_quarter,
  dd.month_name,
  -- Fraud type
  ffe.fraud_type,
  -- Channel
  ch.channel_name,
  ch.channel_category,
  ch.is_digital,
  -- Alert funnel
  COUNT(ffe.fraud_sk)                                    AS total_alerts,
  COUNT(CASE WHEN ffe.is_confirmed_fraud = true THEN 1 END) AS confirmed_fraud_count,
  COUNT(CASE WHEN ffe.is_confirmed_fraud = false THEN 1 END) AS false_positive_count,
  ROUND(
    COUNT(CASE WHEN ffe.is_confirmed_fraud = true THEN 1 END) * 100.0
    / NULLIF(COUNT(ffe.fraud_sk), 0), 2
  )                                                      AS confirmation_rate_pct,
  -- Financial impact
  SUM(ffe.transaction_amount)                            AS at_risk_amount,
  SUM(CASE WHEN ffe.is_confirmed_fraud THEN ffe.loss_amount ELSE 0 END) AS gross_loss_zar,
  SUM(CASE WHEN ffe.is_confirmed_fraud THEN ffe.recovery_amount ELSE 0 END) AS recovery_amount_zar,
  SUM(CASE WHEN ffe.is_confirmed_fraud THEN ffe.net_loss_amount ELSE 0 END) AS net_loss_zar,
  ROUND(
    SUM(CASE WHEN ffe.is_confirmed_fraud THEN ffe.recovery_amount ELSE 0 END) * 100.0
    / NULLIF(SUM(CASE WHEN ffe.is_confirmed_fraud THEN ffe.loss_amount ELSE 0 END), 0), 2
  )                                                      AS recovery_rate_pct,
  -- Resolution performance
  AVG(ffe.resolution_days)                               AS avg_resolution_days,
  COUNT(CASE WHEN ffe.breach_sla_flag = true THEN 1 END) AS sla_breach_count,
  ROUND(
    COUNT(CASE WHEN ffe.breach_sla_flag = true THEN 1 END) * 100.0
    / NULLIF(COUNT(ffe.fraud_sk), 0), 2
  )                                                      AS sla_breach_rate_pct
FROM prime_capital.gold.fact_fraud_event ffe
JOIN prime_capital.gold.dim_date dd
  ON ffe.date_sk = dd.date_sk
JOIN prime_capital.gold.dim_channel ch
  ON ffe.channel_sk = ch.channel_sk
WHERE ffe.alert_date >= DATEADD(DAY, -365, CURRENT_DATE)
GROUP BY
  ffe.alert_date, dd.financial_year, dd.financial_quarter, dd.month_name,
  ffe.fraud_type, ch.channel_name, ch.channel_category, ch.is_digital;


-- ---------------------------------------------------------------------------
-- VIEW 5: vw_card_spending_by_mcc
--    Card spend analysed by merchant category code.
--    Grain: one row per MCC category per month per card_tier.
--    Used by: Card product team, marketing, rewards analytics.
-- ---------------------------------------------------------------------------
DROP VIEW IF EXISTS prime_capital.gold.vw_card_spending_by_mcc;

CREATE OR REPLACE VIEW prime_capital.gold.vw_card_spending_by_mcc
COMMENT 'Card spending analysed by merchant category code (MCC). Monthly grain by MCC category and card tier. Shows transaction volumes, total spend, average transaction size, and rewards points earned per category. Used for card product optimisation, rewards programme design, and marketing targeting.'
AS
SELECT
  -- Time dimension
  DATE_TRUNC('MONTH', fct.transaction_date)              AS spend_month,
  dd.financial_year,
  dd.financial_quarter,
  -- Merchant category
  m.mcc_code,
  m.mcc_description,
  m.mcc_category,
  m.mcc_category_group,
  -- Card attributes
  fct.card_tier,
  fct.card_scheme,
  -- Customer segment
  c.customer_type,
  c.income_band,
  -- Geography
  m.merchant_country,
  m.is_domestic,
  -- Volume
  COUNT(fct.card_tx_sk)                                  AS transaction_count,
  COUNT(DISTINCT fct.customer_sk)                        AS unique_customers,
  -- Spend
  SUM(fct.billing_amount_zar)                            AS total_spend_zar,
  ROUND(AVG(fct.billing_amount_zar), 2)                  AS avg_transaction_zar,
  SUM(CASE WHEN fct.is_international THEN fct.billing_amount_zar ELSE 0 END) AS international_spend_zar,
  SUM(CASE WHEN NOT fct.is_international THEN fct.billing_amount_zar ELSE 0 END) AS domestic_spend_zar,
  -- Rewards
  SUM(fct.reward_points_earned)                          AS total_points_earned,
  ROUND(SUM(fct.reward_points_earned) / NULLIF(SUM(fct.billing_amount_zar), 0), 4) AS points_per_rand,
  -- Fraud
  COUNT(CASE WHEN fct.fraud_flag = true THEN 1 END)      AS fraud_count,
  SUM(CASE WHEN fct.fraud_flag = true THEN fct.billing_amount_zar ELSE 0 END) AS fraud_amount_zar
FROM prime_capital.gold.fact_card_transaction fct
JOIN prime_capital.gold.dim_merchant m
  ON fct.merchant_sk = m.merchant_sk AND m.is_current = true
JOIN prime_capital.gold.dim_date dd
  ON fct.date_sk = dd.date_sk
JOIN prime_capital.gold.dim_customer c
  ON fct.customer_sk = c.customer_sk AND c.is_current = true
WHERE fct.transaction_status = 'SETTLED'
  AND fct.transaction_type NOT IN ('REFUND', 'REVERSAL')
GROUP BY
  DATE_TRUNC('MONTH', fct.transaction_date), dd.financial_year, dd.financial_quarter,
  m.mcc_code, m.mcc_description, m.mcc_category, m.mcc_category_group,
  fct.card_tier, fct.card_scheme, c.customer_type, c.income_band,
  m.merchant_country, m.is_domestic;


-- ---------------------------------------------------------------------------
-- VIEW 6: vw_payment_flows
--    Inbound and outbound payment volumes by type and date.
--    Grain: one row per payment_type per direction per settlement_date.
--    Used by: Treasury (liquidity), Payments operations, SARB reporting.
-- ---------------------------------------------------------------------------
DROP VIEW IF EXISTS prime_capital.gold.vw_payment_flows;

CREATE OR REPLACE VIEW prime_capital.gold.vw_payment_flows
COMMENT 'Payment flow analysis by payment type (EFT, RTGS, SWIFT, Internal, Debit Order) and direction (Inbound/Outbound). Daily grain. Supports liquidity management, SARB regulatory reporting, correspondent banking, and settlement monitoring. SWIFT cross-border flows reported separately.'
AS
SELECT
  fp.settlement_date,
  dd.financial_year,
  dd.financial_quarter,
  dd.month_name,
  dd.is_business_day,
  -- Payment classification
  fp.payment_type,
  fp.payment_direction,
  -- Channel
  ch.channel_name,
  ch.channel_category,
  -- Cross-border vs domestic
  fp.is_cross_border,
  fp.is_internal,
  -- Currency
  cu.currency_code,
  cu.currency_name,
  -- Volume
  COUNT(fp.payment_sk)                                    AS payment_count,
  COUNT(DISTINCT fp.originator_customer_sk)               AS unique_originators,
  -- Values
  SUM(fp.zar_equivalent)                                  AS total_value_zar,
  SUM(fp.payment_amount)                                  AS total_value_original_currency,
  ROUND(AVG(fp.zar_equivalent), 2)                        AS avg_payment_zar,
  SUM(fp.fee_amount)                                      AS total_fee_revenue_zar,
  -- Settlement performance
  COUNT(CASE WHEN fp.is_settled = true THEN 1 END)        AS settled_count,
  COUNT(CASE WHEN fp.is_failed = true THEN 1 END)         AS failed_count,
  ROUND(
    COUNT(CASE WHEN fp.is_settled = true THEN 1 END) * 100.0
    / NULLIF(COUNT(fp.payment_sk), 0), 2
  )                                                        AS settlement_rate_pct,
  AVG(fp.processing_lag_minutes)                          AS avg_processing_lag_minutes,
  -- Net flow (outbound = negative, inbound = positive for liquidity)
  SUM(
    CASE fp.payment_direction
      WHEN 'INBOUND'  THEN  fp.zar_equivalent
      WHEN 'OUTBOUND' THEN -fp.zar_equivalent
    END
  )                                                        AS net_flow_zar
FROM prime_capital.gold.fact_payment fp
JOIN prime_capital.gold.dim_date dd
  ON fp.date_sk = dd.date_sk
JOIN prime_capital.gold.dim_channel ch
  ON fp.channel_sk = ch.channel_sk
JOIN prime_capital.gold.dim_currency cu
  ON fp.currency_sk = cu.currency_sk
GROUP BY
  fp.settlement_date, dd.financial_year, dd.financial_quarter, dd.month_name, dd.is_business_day,
  fp.payment_type, fp.payment_direction, ch.channel_name, ch.channel_category,
  fp.is_cross_border, fp.is_internal, cu.currency_code, cu.currency_name;


-- ---------------------------------------------------------------------------
-- VIEW 7: vw_branch_performance
--    Branch-level KPIs: deposits, loans, revenue, and headcount.
--    Grain: one row per branch per month.
--    Used by: Branch network, regional managers, EXCO.
-- ---------------------------------------------------------------------------
DROP VIEW IF EXISTS prime_capital.gold.vw_branch_performance;

CREATE OR REPLACE VIEW prime_capital.gold.vw_branch_performance
COMMENT 'Branch performance scorecard. Monthly grain showing total deposits, loan book, transaction volumes, fee revenue, and customer acquisition per branch. Includes geographic context (province, region, area type) and branch metadata. Used by regional managers for branch benchmarking and EXCO for network performance reviews.'
AS
SELECT
  -- Time
  DATE_TRUNC('MONTH', ft.transaction_date)               AS performance_month,
  dd.financial_year,
  dd.financial_quarter,
  -- Branch
  b.branch_code,
  b.branch_name,
  b.branch_type,
  b.province_name,
  b.region_name,
  b.area_classification,
  b.atm_count,
  b.city,
  -- Transaction performance
  COUNT(ft.transaction_sk)                               AS total_transactions,
  COUNT(DISTINCT ft.customer_sk)                         AS active_customers,
  SUM(ft.amount_zar)                                     AS total_transaction_value_zar,
  SUM(ft.fee_amount_zar)                                 AS total_fee_revenue_zar,
  SUM(CASE WHEN ft.tx_category = 'CREDIT' THEN ft.amount_zar ELSE 0 END) AS total_deposits_received,
  SUM(CASE WHEN ft.tx_category = 'DEBIT' THEN ft.amount_zar ELSE 0 END)  AS total_withdrawals,
  -- Average deposit balance for the month (from account balance fact)
  SUM(fab.closing_balance)                               AS total_branch_deposit_balance,
  AVG(fab.closing_balance)                               AS avg_daily_deposit_balance,
  -- Loan book (current snapshot — joined for the last day of month)
  SUM(flp.outstanding_balance)                           AS total_loan_book,
  SUM(flp.arrears_amount)                                AS total_arrears,
  ROUND(SUM(flp.arrears_amount) / NULLIF(SUM(flp.outstanding_balance), 0) * 100, 4)
                                                         AS arrears_rate_pct,
  COUNT(CASE WHEN flp.npl_flag = true THEN 1 END)        AS npl_count,
  SUM(CASE WHEN flp.npl_flag = true THEN flp.outstanding_balance ELSE 0 END) AS npl_balance
FROM prime_capital.gold.fact_transaction ft
JOIN prime_capital.gold.dim_branch b
  ON ft.branch_sk = b.branch_sk AND b.is_current = true
JOIN prime_capital.gold.dim_date dd
  ON ft.date_sk = dd.date_sk
-- Join account balance for deposit book size
LEFT JOIN prime_capital.gold.fact_account_balance fab
  ON ft.account_sk = fab.account_sk
  AND fab.snapshot_date = ft.transaction_date
-- Join loan portfolio for NPL at branch level
LEFT JOIN prime_capital.gold.fact_loan_portfolio flp
  ON flp.branch_sk = b.branch_sk
  AND flp.snapshot_date = ft.transaction_date
  AND flp.loan_status NOT IN ('SETTLED', 'WRITTEN_OFF')
GROUP BY
  DATE_TRUNC('MONTH', ft.transaction_date), dd.financial_year, dd.financial_quarter,
  b.branch_code, b.branch_name, b.branch_type, b.province_name,
  b.region_name, b.area_classification, b.atm_count, b.city;


-- ---------------------------------------------------------------------------
-- VIEW 8: vw_treasury_pnl
--    Treasury profit and loss by instrument type and trading desk.
--    Grain: one row per desk per instrument_type per month.
--    Used by: Treasury front office, CFO, ALCO.
-- ---------------------------------------------------------------------------
DROP VIEW IF EXISTS prime_capital.gold.vw_treasury_pnl;

CREATE OR REPLACE VIEW prime_capital.gold.vw_treasury_pnl
COMMENT 'Treasury P&L view aggregated by trading desk, instrument type, and book (FVTPL/AFS/HTM). Monthly grain. Shows notional exposure, mark-to-market value, unrealised and realised P&L, accrued interest, and portfolio duration/DV01 sensitivities. Used by Treasury front office and ALCO for portfolio performance and risk monitoring.'
AS
SELECT
  -- Time
  DATE_TRUNC('MONTH', ftp.snapshot_date)                 AS pnl_month,
  dd.financial_year,
  dd.financial_quarter,
  -- Desk and book
  ftp.desk,
  ftp.book_id,
  -- Instrument
  ftp.trade_type,
  ftp.instrument_type,
  -- Currency
  cu.currency_code,
  cu.currency_name,
  -- Portfolio metrics
  COUNT(DISTINCT ftp.trade_id)                            AS trade_count,
  SUM(CASE WHEN ftp.is_live THEN 1 ELSE 0 END)           AS live_trade_count,
  -- Exposure
  SUM(ftp.notional_amount_zar)                           AS total_notional_zar,
  SUM(ftp.mtm_value_zar)                                 AS total_mtm_value_zar,
  -- P&L
  SUM(ftp.unrealised_pnl)                                AS total_unrealised_pnl,
  SUM(ftp.realised_pnl)                                  AS total_realised_pnl,
  SUM(ftp.total_pnl)                                     AS total_pnl,
  SUM(ftp.accrued_interest)                              AS total_accrued_interest,
  -- Risk sensitivities
  SUM(ftp.dv01)                                          AS portfolio_dv01,
  AVG(ftp.duration)                                      AS avg_duration,
  AVG(ftp.yield)                                         AS avg_yield,
  -- Days to maturity (weighted by notional)
  ROUND(
    SUM(ftp.days_to_maturity * ftp.notional_amount_zar)
    / NULLIF(SUM(ftp.notional_amount_zar), 0)
  , 0)                                                   AS weighted_avg_days_to_maturity,
  -- Return on notional
  ROUND(
    SUM(ftp.total_pnl) / NULLIF(SUM(ftp.notional_amount_zar), 0) * 100
  , 4)                                                   AS return_on_notional_pct
FROM prime_capital.gold.fact_treasury_position ftp
JOIN prime_capital.gold.dim_date dd
  ON ftp.date_sk = dd.date_sk
JOIN prime_capital.gold.dim_currency cu
  ON ftp.currency_sk = cu.currency_sk
GROUP BY
  DATE_TRUNC('MONTH', ftp.snapshot_date), dd.financial_year, dd.financial_quarter,
  ftp.desk, ftp.book_id, ftp.trade_type, ftp.instrument_type,
  cu.currency_code, cu.currency_name;


-- ---------------------------------------------------------------------------
-- VIEW 9: vw_aml_risk_summary
--    AML case pipeline, SAR statistics, and regulatory SLA compliance.
--    Grain: one row per alert_type per month.
--    Used by: AML/Compliance team, FIC regulatory reporting.
-- ---------------------------------------------------------------------------
DROP VIEW IF EXISTS prime_capital.gold.vw_aml_risk_summary;

CREATE OR REPLACE VIEW prime_capital.gold.vw_aml_risk_summary
COMMENT 'AML risk summary and SAR pipeline statistics. Monthly grain by AML typology and priority. Covers alert volumes, SAR filing rates, amounts reported to FIC, average review durations, and regulatory SLA compliance. Used by compliance officers for monthly Board reporting and FIC submissions.'
AS
SELECT
  -- Time
  DATE_TRUNC('MONTH', fac.alert_date)                    AS alert_month,
  dd.financial_year,
  dd.financial_quarter,
  -- Alert classification
  fac.alert_type,
  fac.alert_priority,
  -- Customer risk
  rc.risk_tier_name,
  -- Funnel metrics
  COUNT(fac.aml_case_sk)                                 AS total_alerts,
  COUNT(CASE WHEN fac.case_status = 'OPEN' THEN 1 END)           AS open_cases,
  COUNT(CASE WHEN fac.case_status = 'UNDER_REVIEW' THEN 1 END)   AS cases_under_review,
  COUNT(CASE WHEN fac.case_status = 'CLOSED_NO_ACTION' THEN 1 END) AS closed_no_action,
  COUNT(CASE WHEN fac.case_status = 'SAR_FILED' THEN 1 END)      AS sar_filed_count,
  COUNT(CASE WHEN fac.case_status = 'REFERRED' THEN 1 END)       AS referred_count,
  -- SAR amounts
  SUM(CASE WHEN fac.sar_filed_flag = true THEN fac.sar_amount ELSE 0 END) AS total_sar_amount_zar,
  ROUND(
    COUNT(CASE WHEN fac.sar_filed_flag = true THEN 1 END) * 100.0
    / NULLIF(COUNT(fac.aml_case_sk), 0), 2
  )                                                      AS sar_rate_pct,
  -- Amounts flagged
  SUM(fac.total_amount_flagged)                          AS total_amount_flagged_zar,
  AVG(fac.risk_score)                                    AS avg_risk_score,
  -- Transaction patterns
  AVG(fac.transaction_count)                             AS avg_transactions_per_case,
  AVG(fac.lookback_period_days)                          AS avg_lookback_days,
  -- Review performance
  AVG(CASE WHEN fac.review_duration_days IS NOT NULL
        THEN fac.review_duration_days END)               AS avg_review_duration_days,
  COUNT(CASE WHEN fac.breach_regulatory_sla = true THEN 1 END) AS regulatory_sla_breaches,
  ROUND(
    COUNT(CASE WHEN fac.breach_regulatory_sla = true THEN 1 END) * 100.0
    / NULLIF(COUNT(fac.aml_case_sk), 0), 2
  )                                                      AS sla_breach_rate_pct
FROM prime_capital.gold.fact_aml_case fac
JOIN prime_capital.gold.dim_date dd
  ON fac.date_sk = dd.date_sk
JOIN prime_capital.gold.dim_risk_category rc
  ON fac.risk_category_sk = rc.risk_category_sk
GROUP BY
  DATE_TRUNC('MONTH', fac.alert_date), dd.financial_year, dd.financial_quarter,
  fac.alert_type, fac.alert_priority, rc.risk_tier_name;


-- ---------------------------------------------------------------------------
-- VIEW 10: vw_income_statement
--    Profit & Loss constructed from the general ledger.
--    Grain: one row per GL account per accounting period per legal entity.
--    Used by: Finance, CFO office, Board reporting, SARB returns.
-- ---------------------------------------------------------------------------
DROP VIEW IF EXISTS prime_capital.gold.vw_income_statement;

CREATE OR REPLACE VIEW prime_capital.gold.vw_income_statement
COMMENT 'Income statement (P&L) view constructed from the general ledger. Shows net interest income, non-interest income, operating expenses, credit impairment charges, and profit before/after tax by accounting period and legal entity. Built on the 5-level COA hierarchy. Used for monthly Board pack, CFO reporting, and SARB regulatory returns.'
AS
SELECT
  -- Period
  fgj.accounting_period,
  fgj.financial_year,
  dd.financial_quarter,
  -- Entity and organisation
  fgj.legal_entity,
  fgj.business_unit,
  fgj.cost_centre_code,
  -- GL hierarchy
  ga.level2_name                                         AS segment,
  ga.level3_name                                         AS division,
  ga.account_type,
  ga.gl_account_code,
  ga.gl_account_name,
  -- Income and expense classification
  CASE
    WHEN ga.gl_account_name LIKE '%Interest Income%'          THEN 'NET_INTEREST_INCOME'
    WHEN ga.gl_account_name LIKE '%Interest Expense%'         THEN 'NET_INTEREST_INCOME'
    WHEN ga.gl_account_name LIKE '%Fee Income%'               THEN 'NON_INTEREST_INCOME'
    WHEN ga.gl_account_name LIKE '%Commission%'               THEN 'NON_INTEREST_INCOME'
    WHEN ga.gl_account_name LIKE '%Trading Income%'           THEN 'NON_INTEREST_INCOME'
    WHEN ga.gl_account_name LIKE '%Impairment%'               THEN 'CREDIT_IMPAIRMENT'
    WHEN ga.gl_account_name LIKE '%Provision%'                THEN 'CREDIT_IMPAIRMENT'
    WHEN ga.gl_account_name LIKE '%Staff%'
      OR ga.gl_account_name LIKE '%Salaries%'                 THEN 'STAFF_COSTS'
    WHEN ga.gl_account_name LIKE '%IT%'
      OR ga.gl_account_name LIKE '%Technology%'               THEN 'IT_COSTS'
    WHEN ga.gl_account_name LIKE '%Property%'
      OR ga.gl_account_name LIKE '%Rent%'                     THEN 'PROPERTY_COSTS'
    WHEN ga.account_type = 'EXPENSE'                          THEN 'OTHER_OPERATING_EXPENSES'
    WHEN ga.gl_account_name LIKE '%Tax%'                      THEN 'INCOME_TAX'
    ELSE 'OTHER'
  END                                                    AS pnl_line_item,
  -- Amounts
  SUM(fgj.credit_amount)                                 AS total_credits,
  SUM(fgj.debit_amount)                                  AS total_debits,
  SUM(fgj.net_amount)                                    AS net_amount,
  -- For income accounts: credit = revenue; for expense: debit = cost
  CASE ga.normal_balance
    WHEN 'CREDIT' THEN SUM(fgj.net_amount)   -- Income account (positive = income)
    WHEN 'DEBIT'  THEN -SUM(fgj.net_amount)  -- Expense account (positive = cost)
  END                                                    AS pnl_contribution
FROM prime_capital.gold.fact_gl_journal fgj
JOIN prime_capital.gold.dim_gl_account ga
  ON fgj.gl_account_sk = ga.gl_account_sk AND ga.is_current = true
JOIN prime_capital.gold.dim_date dd
  ON fgj.date_sk = dd.date_sk
WHERE ga.is_income_statement = true
  AND fgj.is_reversal = false  -- Exclude reversal entries (net already in original)
GROUP BY
  fgj.accounting_period, fgj.financial_year, dd.financial_quarter,
  fgj.legal_entity, fgj.business_unit, fgj.cost_centre_code,
  ga.level2_name, ga.level3_name, ga.account_type,
  ga.gl_account_code, ga.gl_account_name, ga.normal_balance;


-- ---------------------------------------------------------------------------
-- VIEW 11: vw_balance_sheet
--    Assets, liabilities, and equity from the general ledger.
--    Grain: one row per GL account per month-end date per legal entity.
--    Used by: Finance, CFO, SARB BA900 returns, external audit.
-- ---------------------------------------------------------------------------
DROP VIEW IF EXISTS prime_capital.gold.vw_balance_sheet;

CREATE OR REPLACE VIEW prime_capital.gold.vw_balance_sheet
COMMENT 'Balance sheet view constructed from the general ledger at month-end. Shows total assets (cash, loans, investments, property), total liabilities (deposits, borrowings, subordinated debt), and equity by legal entity and segment. Used for SARB BA100 returns, IFRS financial statements, and Board-level financial reporting.'
AS
SELECT
  -- Period (month-end only)
  fgj.accounting_period,
  fgj.financial_year,
  dd.financial_quarter,
  -- Entity
  fgj.legal_entity,
  -- GL hierarchy
  ga.level2_name                                         AS segment,
  ga.account_type,
  -- Balance sheet classification
  CASE ga.account_type
    WHEN 'ASSET'     THEN
      CASE
        WHEN ga.gl_account_name LIKE '%Cash%'
          OR ga.gl_account_name LIKE '%Nostro%'           THEN 'CASH_AND_EQUIVALENTS'
        WHEN ga.gl_account_name LIKE '%Loan%'             THEN 'LOANS_AND_ADVANCES'
        WHEN ga.gl_account_name LIKE '%Investment%'
          OR ga.gl_account_name LIKE '%Bond%'
          OR ga.gl_account_name LIKE '%Treasury%'         THEN 'INVESTMENT_SECURITIES'
        WHEN ga.gl_account_name LIKE '%Interbank%'        THEN 'INTERBANK_PLACEMENTS'
        WHEN ga.gl_account_name LIKE '%Property%'
          OR ga.gl_account_name LIKE '%Equipment%'        THEN 'FIXED_ASSETS'
        ELSE 'OTHER_ASSETS'
      END
    WHEN 'LIABILITY' THEN
      CASE
        WHEN ga.gl_account_name LIKE '%Deposit%'
          OR ga.gl_account_name LIKE '%Savings%'
          OR ga.gl_account_name LIKE '%Current%'          THEN 'CUSTOMER_DEPOSITS'
        WHEN ga.gl_account_name LIKE '%Wholesale%'
          OR ga.gl_account_name LIKE '%Money Market%'     THEN 'WHOLESALE_FUNDING'
        WHEN ga.gl_account_name LIKE '%Subordinated%'     THEN 'SUBORDINATED_DEBT'
        WHEN ga.gl_account_name LIKE '%Interbank%'        THEN 'INTERBANK_BORROWINGS'
        ELSE 'OTHER_LIABILITIES'
      END
    WHEN 'EQUITY'    THEN 'EQUITY'
    ELSE 'OTHER'
  END                                                    AS bs_line_item,
  ga.gl_account_code,
  ga.gl_account_name,
  -- Balance (cumulative to month-end for BS accounts)
  SUM(fgj.credit_amount)                                 AS total_credits,
  SUM(fgj.debit_amount)                                  AS total_debits,
  -- Balance sheet balance: Assets = DR normal, Liabilities/Equity = CR normal
  CASE ga.normal_balance
    WHEN 'DEBIT'  THEN  SUM(fgj.debit_amount) - SUM(fgj.credit_amount)
    WHEN 'CREDIT' THEN  SUM(fgj.credit_amount) - SUM(fgj.debit_amount)
  END                                                    AS closing_balance
FROM prime_capital.gold.fact_gl_journal fgj
JOIN prime_capital.gold.dim_gl_account ga
  ON fgj.gl_account_sk = ga.gl_account_sk AND ga.is_current = true
JOIN prime_capital.gold.dim_date dd
  ON fgj.date_sk = dd.date_sk
WHERE ga.is_balance_sheet = true
  AND dd.is_month_end = true  -- Month-end closing balances only
GROUP BY
  fgj.accounting_period, fgj.financial_year, dd.financial_quarter,
  fgj.legal_entity, ga.level2_name, ga.account_type,
  ga.gl_account_code, ga.gl_account_name, ga.normal_balance;


-- ---------------------------------------------------------------------------
-- VIEW 12: vw_capital_adequacy
--    Basel III Tier 1 and Tier 2 capital ratios.
--    Grain: one row per reporting period per legal entity.
--    Used by: Risk, SARB regulatory reporting (BA700), ALCO.
-- ---------------------------------------------------------------------------
DROP VIEW IF EXISTS prime_capital.gold.vw_capital_adequacy;

CREATE OR REPLACE VIEW prime_capital.gold.vw_capital_adequacy
COMMENT 'Basel III capital adequacy view. Computes Common Equity Tier 1 (CET1), Additional Tier 1 (AT1), Tier 1, Tier 2, and Total Capital ratios against Risk-Weighted Assets. Benchmarks against SARB minimum requirements: CET1 >= 7%, T1 >= 8.5%, Total CAR >= 10.5% (including buffers). Used for SARB BA700 regulatory returns and ALCO capital planning.'
AS
WITH capital_components AS (
  -- Extract capital and RWA components from the GL using account classifications
  SELECT
    fgj.accounting_period,
    fgj.financial_year,
    fgj.legal_entity,
    -- CET1: Ordinary share capital + retained earnings + regulatory adjustments
    SUM(CASE WHEN ga.gl_account_name LIKE '%Ordinary Share Capital%'   THEN ABS(fgj.net_amount) ELSE 0 END) AS ordinary_share_capital,
    SUM(CASE WHEN ga.gl_account_name LIKE '%Share Premium%'            THEN ABS(fgj.net_amount) ELSE 0 END) AS share_premium,
    SUM(CASE WHEN ga.gl_account_name LIKE '%Retained Earnings%'        THEN ABS(fgj.net_amount) ELSE 0 END) AS retained_earnings,
    SUM(CASE WHEN ga.gl_account_name LIKE '%Other Comprehensive Income%' THEN ABS(fgj.net_amount) ELSE 0 END) AS oci_reserves,
    -- Deductions from CET1
    SUM(CASE WHEN ga.gl_account_name LIKE '%Goodwill%'                 THEN ABS(fgj.net_amount) ELSE 0 END) AS goodwill,
    SUM(CASE WHEN ga.gl_account_name LIKE '%Deferred Tax Asset%'       THEN ABS(fgj.net_amount) ELSE 0 END) AS deferred_tax_assets,
    -- AT1: Preference shares and hybrid instruments
    SUM(CASE WHEN ga.gl_account_name LIKE '%Preference Share%'         THEN ABS(fgj.net_amount) ELSE 0 END) AS at1_instruments,
    -- Tier 2: Subordinated debt + general provisions
    SUM(CASE WHEN ga.gl_account_name LIKE '%Subordinated%'             THEN ABS(fgj.net_amount) ELSE 0 END) AS subordinated_debt,
    SUM(CASE WHEN ga.gl_account_name LIKE '%General Provision%'        THEN ABS(fgj.net_amount) ELSE 0 END) AS general_provisions,
    -- Risk-Weighted Assets proxy from credit risk-weighted exposures
    SUM(CASE WHEN ga.gl_account_name LIKE '%Credit RWA%'               THEN ABS(fgj.net_amount) ELSE 0 END) AS credit_rwa,
    SUM(CASE WHEN ga.gl_account_name LIKE '%Market RWA%'               THEN ABS(fgj.net_amount) ELSE 0 END) AS market_rwa,
    SUM(CASE WHEN ga.gl_account_name LIKE '%Operational RWA%'          THEN ABS(fgj.net_amount) ELSE 0 END) AS operational_rwa
  FROM prime_capital.gold.fact_gl_journal fgj
  JOIN prime_capital.gold.dim_gl_account ga
    ON fgj.gl_account_sk = ga.gl_account_sk AND ga.is_current = true
  JOIN prime_capital.gold.dim_date dd
    ON fgj.date_sk = dd.date_sk
  WHERE dd.is_month_end = true
  GROUP BY fgj.accounting_period, fgj.financial_year, fgj.legal_entity
)
SELECT
  cc.accounting_period,
  cc.financial_year,
  cc.legal_entity,
  -- Capital tier construction
  (cc.ordinary_share_capital + cc.share_premium + cc.retained_earnings + cc.oci_reserves
   - cc.goodwill - cc.deferred_tax_assets)                              AS cet1_capital,
  cc.at1_instruments                                                     AS at1_capital,
  (cc.ordinary_share_capital + cc.share_premium + cc.retained_earnings + cc.oci_reserves
   - cc.goodwill - cc.deferred_tax_assets + cc.at1_instruments)          AS tier1_capital,
  (cc.subordinated_debt + cc.general_provisions)                         AS tier2_capital,
  (cc.ordinary_share_capital + cc.share_premium + cc.retained_earnings + cc.oci_reserves
   - cc.goodwill - cc.deferred_tax_assets + cc.at1_instruments
   + cc.subordinated_debt + cc.general_provisions)                       AS total_capital,
  -- Risk-weighted assets
  (cc.credit_rwa + cc.market_rwa + cc.operational_rwa)                  AS total_rwa,
  cc.credit_rwa,
  cc.market_rwa,
  cc.operational_rwa,
  -- Capital adequacy ratios (as percentages)
  ROUND(
    (cc.ordinary_share_capital + cc.share_premium + cc.retained_earnings + cc.oci_reserves
     - cc.goodwill - cc.deferred_tax_assets)
    / NULLIF((cc.credit_rwa + cc.market_rwa + cc.operational_rwa), 0) * 100
  , 4)                                                                   AS cet1_ratio_pct,
  ROUND(
    (cc.ordinary_share_capital + cc.share_premium + cc.retained_earnings + cc.oci_reserves
     - cc.goodwill - cc.deferred_tax_assets + cc.at1_instruments)
    / NULLIF((cc.credit_rwa + cc.market_rwa + cc.operational_rwa), 0) * 100
  , 4)                                                                   AS tier1_ratio_pct,
  ROUND(
    (cc.ordinary_share_capital + cc.share_premium + cc.retained_earnings + cc.oci_reserves
     - cc.goodwill - cc.deferred_tax_assets + cc.at1_instruments
     + cc.subordinated_debt + cc.general_provisions)
    / NULLIF((cc.credit_rwa + cc.market_rwa + cc.operational_rwa), 0) * 100
  , 4)                                                                   AS total_car_pct,
  -- SARB minimum benchmarks (including capital conservation buffer)
  7.0   AS sarb_minimum_cet1_pct,
  8.5   AS sarb_minimum_tier1_pct,
  10.5  AS sarb_minimum_total_car_pct,
  -- Compliance flags
  ROUND(
    (cc.ordinary_share_capital + cc.share_premium + cc.retained_earnings + cc.oci_reserves
     - cc.goodwill - cc.deferred_tax_assets)
    / NULLIF((cc.credit_rwa + cc.market_rwa + cc.operational_rwa), 0) * 100
  , 4) >= 7.0                                                            AS cet1_compliant,
  ROUND(
    (cc.ordinary_share_capital + cc.share_premium + cc.retained_earnings + cc.oci_reserves
     - cc.goodwill - cc.deferred_tax_assets + cc.at1_instruments
     + cc.subordinated_debt + cc.general_provisions)
    / NULLIF((cc.credit_rwa + cc.market_rwa + cc.operational_rwa), 0) * 100
  , 4) >= 10.5                                                           AS total_car_compliant
FROM capital_components cc;


-- ---------------------------------------------------------------------------
-- VIEW 13: vw_liquidity_ratios
--    Liquidity Coverage Ratio (LCR) and Net Stable Funding Ratio (NSFR).
--    Grain: one row per reporting date per legal entity.
--    Used by: Treasury, Risk, SARB BA300 returns, ALCO.
-- ---------------------------------------------------------------------------
DROP VIEW IF EXISTS prime_capital.gold.vw_liquidity_ratios;

CREATE OR REPLACE VIEW prime_capital.gold.vw_liquidity_ratios
COMMENT 'Basel III liquidity ratios: LCR and NSFR. LCR = High Quality Liquid Assets / Net Cash Outflows over 30 days (minimum 100%). NSFR = Available Stable Funding / Required Stable Funding (minimum 100%). Computed from GL-derived balance sheet components. Used for SARB BA300 monthly returns and ALCO liquidity management.'
AS
WITH liquidity_components AS (
  SELECT
    fgj.accounting_period,
    fgj.financial_year,
    fgj.legal_entity,
    -- HQLA (High Quality Liquid Assets): Level 1 = cash + sovereign bonds; Level 2 = other liquid assets
    SUM(CASE WHEN ga.gl_account_name LIKE '%Cash and Balances%'
          OR ga.gl_account_name LIKE '%SARB Deposits%'          THEN ABS(fgj.net_amount) ELSE 0 END) AS hqla_level1,
    SUM(CASE WHEN ga.gl_account_name LIKE '%Government Bond%'
          OR ga.gl_account_name LIKE '%RSA Bond%'               THEN ABS(fgj.net_amount) * 0.95 ELSE 0 END) AS hqla_level1_bonds,
    SUM(CASE WHEN ga.gl_account_name LIKE '%Corporate Bond%'    THEN ABS(fgj.net_amount) * 0.85 ELSE 0 END) AS hqla_level2a,
    -- Net cash outflows over 30 days (modelled from deposit run-off rates)
    SUM(CASE WHEN ga.gl_account_name LIKE '%Retail Deposit%'    THEN ABS(fgj.net_amount) * 0.05 ELSE 0 END) AS retail_deposit_outflow,
    SUM(CASE WHEN ga.gl_account_name LIKE '%Wholesale Deposit%' THEN ABS(fgj.net_amount) * 0.25 ELSE 0 END) AS wholesale_deposit_outflow,
    SUM(CASE WHEN ga.gl_account_name LIKE '%Corporate Deposit%' THEN ABS(fgj.net_amount) * 0.40 ELSE 0 END) AS corporate_deposit_outflow,
    -- Available Stable Funding (NSFR numerator)
    SUM(CASE WHEN ga.gl_account_name LIKE '%Retail Deposit%'    THEN ABS(fgj.net_amount) * 0.95 ELSE 0 END) AS asf_retail_deposits,
    SUM(CASE WHEN ga.gl_account_name LIKE '%Wholesale Deposit%' THEN ABS(fgj.net_amount) * 0.50 ELSE 0 END) AS asf_wholesale,
    SUM(CASE WHEN ga.gl_account_name LIKE '%Subordinated%'
          OR ga.gl_account_name LIKE '%Tier 1 Capital%'         THEN ABS(fgj.net_amount) * 1.00 ELSE 0 END) AS asf_long_term_funding,
    -- Required Stable Funding (NSFR denominator) — weighted by asset liquidity
    SUM(CASE WHEN ga.gl_account_name LIKE '%Loan%'              THEN ABS(fgj.net_amount) * 0.85 ELSE 0 END) AS rsf_loans,
    SUM(CASE WHEN ga.gl_account_name LIKE '%Property%'
          OR ga.gl_account_name LIKE '%Equipment%'              THEN ABS(fgj.net_amount) * 1.00 ELSE 0 END) AS rsf_fixed_assets
  FROM prime_capital.gold.fact_gl_journal fgj
  JOIN prime_capital.gold.dim_gl_account ga
    ON fgj.gl_account_sk = ga.gl_account_sk AND ga.is_current = true
  JOIN prime_capital.gold.dim_date dd
    ON fgj.date_sk = dd.date_sk
  WHERE dd.is_month_end = true
  GROUP BY fgj.accounting_period, fgj.financial_year, fgj.legal_entity
)
SELECT
  lc.accounting_period,
  lc.financial_year,
  lc.legal_entity,
  -- LCR components
  (lc.hqla_level1 + lc.hqla_level1_bonds)               AS total_hqla_level1,
  lc.hqla_level2a                                        AS total_hqla_level2a,
  (lc.hqla_level1 + lc.hqla_level1_bonds + lc.hqla_level2a) AS total_hqla,
  (lc.retail_deposit_outflow + lc.wholesale_deposit_outflow
   + lc.corporate_deposit_outflow)                        AS total_net_cash_outflows_30d,
  -- LCR ratio
  ROUND(
    (lc.hqla_level1 + lc.hqla_level1_bonds + lc.hqla_level2a)
    / NULLIF((lc.retail_deposit_outflow + lc.wholesale_deposit_outflow
              + lc.corporate_deposit_outflow), 0) * 100
  , 2)                                                   AS lcr_pct,
  100                                                    AS sarb_minimum_lcr_pct,
  -- LCR compliance
  ROUND(
    (lc.hqla_level1 + lc.hqla_level1_bonds + lc.hqla_level2a)
    / NULLIF((lc.retail_deposit_outflow + lc.wholesale_deposit_outflow
              + lc.corporate_deposit_outflow), 0) * 100
  , 2) >= 100                                            AS lcr_compliant,
  -- NSFR components
  (lc.asf_retail_deposits + lc.asf_wholesale + lc.asf_long_term_funding) AS total_asf,
  (lc.rsf_loans + lc.rsf_fixed_assets)                  AS total_rsf,
  -- NSFR ratio
  ROUND(
    (lc.asf_retail_deposits + lc.asf_wholesale + lc.asf_long_term_funding)
    / NULLIF((lc.rsf_loans + lc.rsf_fixed_assets), 0) * 100
  , 2)                                                   AS nsfr_pct,
  100                                                    AS sarb_minimum_nsfr_pct,
  ROUND(
    (lc.asf_retail_deposits + lc.asf_wholesale + lc.asf_long_term_funding)
    / NULLIF((lc.rsf_loans + lc.rsf_fixed_assets), 0) * 100
  , 2) >= 100                                            AS nsfr_compliant
FROM liquidity_components lc;


-- ---------------------------------------------------------------------------
-- VIEW 14: vw_product_profitability
--    Revenue and cost per product for margin analysis.
--    Grain: one row per product per accounting_period.
--    Used by: Product management, Finance, pricing decisions.
-- ---------------------------------------------------------------------------
DROP VIEW IF EXISTS prime_capital.gold.vw_product_profitability;

CREATE OR REPLACE VIEW prime_capital.gold.vw_product_profitability
COMMENT 'Product profitability view. Monthly grain by product showing interest income, fee income, cost of funds, direct costs, contribution margin, and return on allocated capital. Combines transaction fee data, interest accruals from the loan/account books, and cost allocations from the GL. Used by product managers for pricing reviews and portfolio mix decisions.'
AS
WITH product_interest_income AS (
  -- Interest income from loans by product
  SELECT
    p.product_code,
    p.product_name,
    p.product_category,
    p.product_type,
    flp.snapshot_date,
    DATE_TRUNC('MONTH', flp.snapshot_date)               AS income_month,
    SUM(flp.interest_accrued)                            AS interest_income,
    SUM(flp.outstanding_balance)                         AS loan_book_balance,
    SUM(flp.provision_amount)                            AS provision_charge,
    COUNT(DISTINCT flp.loan_id)                          AS loan_count
  FROM prime_capital.gold.fact_loan_portfolio flp
  JOIN prime_capital.gold.dim_product p
    ON flp.product_sk = p.product_sk AND p.is_current = true
  WHERE flp.snapshot_date = (SELECT MAX(snapshot_date) FROM prime_capital.gold.fact_loan_portfolio)
  GROUP BY p.product_code, p.product_name, p.product_category, p.product_type,
           flp.snapshot_date, DATE_TRUNC('MONTH', flp.snapshot_date)
),
product_fee_income AS (
  -- Fee income from transactions by product
  SELECT
    p.product_code,
    DATE_TRUNC('MONTH', ft.transaction_date)             AS income_month,
    SUM(ft.fee_amount_zar)                               AS fee_income,
    COUNT(ft.transaction_sk)                             AS transaction_count,
    COUNT(DISTINCT ft.customer_sk)                       AS active_customers
  FROM prime_capital.gold.fact_transaction ft
  JOIN prime_capital.gold.dim_product p
    ON ft.product_sk = p.product_sk AND p.is_current = true
  GROUP BY p.product_code, DATE_TRUNC('MONTH', ft.transaction_date)
)
SELECT
  COALESCE(pii.income_month, pfi.income_month)           AS reporting_month,
  p.product_code,
  p.product_name,
  p.product_category,
  p.product_type,
  p.product_sub_type,
  -- Income
  COALESCE(pii.interest_income, 0)                       AS interest_income_zar,
  COALESCE(pfi.fee_income, 0)                            AS fee_income_zar,
  COALESCE(pii.interest_income, 0)
    + COALESCE(pfi.fee_income, 0)                        AS total_income_zar,
  -- Volume
  COALESCE(pii.loan_book_balance, 0)                     AS loan_book_balance,
  COALESCE(pii.loan_count, 0)                            AS loan_count,
  COALESCE(pfi.transaction_count, 0)                     AS transaction_count,
  COALESCE(pfi.active_customers, 0)                      AS active_customers,
  -- Credit costs
  COALESCE(pii.provision_charge, 0)                      AS provision_charge_zar,
  -- Cost of funds (assumed prime rate basis — approximation)
  COALESCE(pii.loan_book_balance, 0) * 0.0850 / 12      AS cost_of_funds_zar,
  -- Contribution (income - provision - cost of funds; operational costs allocated separately)
  COALESCE(pii.interest_income, 0) + COALESCE(pfi.fee_income, 0)
    - COALESCE(pii.provision_charge, 0)
    - (COALESCE(pii.loan_book_balance, 0) * 0.0850 / 12) AS contribution_margin_zar,
  -- Margin ratio
  ROUND(
    (COALESCE(pii.interest_income, 0) + COALESCE(pfi.fee_income, 0)
     - COALESCE(pii.provision_charge, 0)
     - COALESCE(pii.loan_book_balance, 0) * 0.0850 / 12)
    / NULLIF(COALESCE(pii.loan_book_balance, 0), 0) * 100
  , 4)                                                   AS net_margin_pct
FROM prime_capital.gold.dim_product p
LEFT JOIN product_interest_income pii ON p.product_code = pii.product_code
LEFT JOIN product_fee_income      pfi ON p.product_code = pfi.product_code
                                     AND pfi.income_month = pii.income_month
WHERE p.is_current = true
  AND p.product_status = 'ACTIVE';


-- ---------------------------------------------------------------------------
-- VIEW 15: vw_digital_adoption
--    Digital channel migration and digital banking usage trends.
--    Grain: one row per channel per customer_type per month.
--    Used by: Digital banking team, strategy, marketing, EXCO.
-- ---------------------------------------------------------------------------
DROP VIEW IF EXISTS prime_capital.gold.vw_digital_adoption;

CREATE OR REPLACE VIEW prime_capital.gold.vw_digital_adoption
COMMENT 'Digital adoption and channel migration view. Monthly grain by channel and customer segment. Tracks transaction volumes and values across digital vs physical channels, digital customer penetration rates, feature adoption (USSD/Chatbot/API), and channel switching patterns. Used by the digital banking team for strategic reporting and the EXCO digital transformation dashboard.'
AS
WITH monthly_channel_activity AS (
  SELECT
    DATE_TRUNC('MONTH', ft.transaction_date)             AS activity_month,
    c.customer_type,
    c.age_band,
    c.income_band,
    c.residential_province_name                          AS province,
    ch.channel_code,
    ch.channel_name,
    ch.channel_category,
    ch.is_digital,
    -- Volume
    COUNT(ft.transaction_sk)                             AS tx_count,
    COUNT(DISTINCT ft.customer_sk)                       AS active_customers,
    SUM(ft.amount_zar)                                   AS total_value_zar,
    SUM(ft.fee_amount_zar)                               AS fee_revenue_zar
  FROM prime_capital.gold.fact_transaction ft
  JOIN prime_capital.gold.dim_customer c
    ON ft.customer_sk = c.customer_sk AND c.is_current = true
  JOIN prime_capital.gold.dim_channel ch
    ON ft.channel_sk = ch.channel_sk
  GROUP BY
    DATE_TRUNC('MONTH', ft.transaction_date),
    c.customer_type, c.age_band, c.income_band, c.residential_province_name,
    ch.channel_code, ch.channel_name, ch.channel_category, ch.is_digital
),
monthly_totals AS (
  -- Total per customer_type per month (for penetration rate denominator)
  SELECT
    activity_month,
    customer_type,
    SUM(tx_count)                                        AS total_tx_count,
    SUM(total_value_zar)                                 AS total_value_zar,
    SUM(active_customers)                                AS total_active_customers
  FROM monthly_channel_activity
  GROUP BY activity_month, customer_type
)
SELECT
  mca.activity_month,
  -- Date labels
  DATE_FORMAT(mca.activity_month, 'yyyy-MM')             AS month_label,
  YEAR(mca.activity_month)                               AS calendar_year,
  MONTH(mca.activity_month)                              AS calendar_month,
  -- Dimensions
  mca.customer_type,
  mca.age_band,
  mca.income_band,
  mca.province,
  mca.channel_code,
  mca.channel_name,
  mca.channel_category,
  mca.is_digital,
  -- Volume
  mca.tx_count,
  mca.active_customers,
  mca.total_value_zar,
  mca.fee_revenue_zar,
  -- Channel share within customer type
  ROUND(mca.tx_count * 100.0 / NULLIF(mt.total_tx_count, 0), 2)         AS tx_count_share_pct,
  ROUND(mca.total_value_zar * 100.0 / NULLIF(mt.total_value_zar, 0), 2) AS value_share_pct,
  ROUND(mca.active_customers * 100.0 / NULLIF(mt.total_active_customers, 0), 2) AS customer_penetration_pct,
  -- Digital penetration flag at row level
  mca.is_digital                                         AS is_digital_channel,
  -- Average transaction value
  ROUND(mca.total_value_zar / NULLIF(mca.tx_count, 0), 2) AS avg_transaction_zar,
  -- Fee per transaction (efficiency metric)
  ROUND(mca.fee_revenue_zar / NULLIF(mca.tx_count, 0), 4) AS fee_per_transaction
FROM monthly_channel_activity mca
JOIN monthly_totals mt
  ON mca.activity_month  = mt.activity_month
  AND mca.customer_type  = mt.customer_type
ORDER BY
  mca.activity_month DESC,
  mca.customer_type,
  mca.is_digital DESC,
  mca.tx_count DESC;

-- =============================================================================
-- END OF GOLD ANALYTICAL VIEWS DDL
-- =============================================================================
