-- =============================================================================
-- VW_CUSTOMER_360
-- Unified customer view: demographics, balances, risk profile, and lifetime value
-- =============================================================================
CREATE OR REPLACE VIEW gold.vw_customer_360 AS
SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.first_name || ' ' || c.last_name                          AS full_name,
    c.id_number,
    c.date_of_birth,
    DATEDIFF(YEAR, c.date_of_birth, CURRENT_DATE)               AS age,
    CASE
        WHEN DATEDIFF(YEAR, c.date_of_birth, CURRENT_DATE) < 30  THEN '18-29'
        WHEN DATEDIFF(YEAR, c.date_of_birth, CURRENT_DATE) < 40  THEN '30-39'
        WHEN DATEDIFF(YEAR, c.date_of_birth, CURRENT_DATE) < 55  THEN '40-54'
        ELSE '55+'
    END                                                          AS age_band,
    c.gender,
    c.province,
    c.city,
    c.customer_segment,
    c.employment_status,
    c.annual_income_zar,
    c.credit_score,
    CASE
        WHEN c.credit_score >= 750 THEN 'Excellent'
        WHEN c.credit_score >= 680 THEN 'Good'
        WHEN c.credit_score >= 620 THEN 'Fair'
        WHEN c.credit_score >= 580 THEN 'Poor'
        ELSE 'Very Poor'
    END                                                          AS credit_band,
    c.kyc_status,
    c.onboarding_date,
    DATEDIFF(YEAR, c.onboarding_date, CURRENT_DATE)             AS tenure_years,

    -- Account aggregates
    acc.total_accounts,
    acc.total_balance_zar,
    acc.savings_balance_zar,
    acc.cheque_balance_zar,
    acc.investment_balance_zar,

    -- Loan aggregates
    ln.total_loans,
    ln.total_outstanding_zar,
    ln.total_credit_limit_zar,
    ROUND(ln.total_outstanding_zar / NULLIF(ln.total_credit_limit_zar, 0) * 100, 2) AS utilisation_pct,

    -- Transaction activity (last 12 months)
    tx.monthly_tx_count,
    tx.monthly_tx_value_zar,
    tx.avg_tx_value_zar,

    -- Fraud flag
    fr.fraud_alert_count,
    fr.active_fraud_flag,

    -- Derived metrics
    ROUND(
        (c.credit_score / 850.0) * 0.30
        + LEAST(acc.total_balance_zar / 500000.0, 1.0) * 0.25
        + LEAST(tx.monthly_tx_value_zar / 50000.0, 1.0) * 0.25
        + (1 - LEAST(COALESCE(ln.total_outstanding_zar, 0) / NULLIF(ln.total_credit_limit_zar, 1), 1.0)) * 0.20
    , 4)                                                         AS clv_score,

    c.is_active

FROM dim_customer c

LEFT JOIN (
    SELECT customer_id,
           COUNT(*)                                              AS total_accounts,
           SUM(current_balance_zar)                             AS total_balance_zar,
           SUM(CASE WHEN account_type='Savings'     THEN current_balance_zar ELSE 0 END) AS savings_balance_zar,
           SUM(CASE WHEN account_type='Cheque'      THEN current_balance_zar ELSE 0 END) AS cheque_balance_zar,
           SUM(CASE WHEN account_type='Investment'  THEN current_balance_zar ELSE 0 END) AS investment_balance_zar
    FROM dim_account
    WHERE account_status = 'Active'
    GROUP BY customer_id
) acc ON c.customer_id = acc.customer_id

LEFT JOIN (
    SELECT customer_id,
           COUNT(*)                         AS total_loans,
           SUM(outstanding_balance_zar)     AS total_outstanding_zar,
           SUM(approved_amount_zar)         AS total_credit_limit_zar
    FROM fact_loan
    WHERE loan_status NOT IN ('Closed', 'Written Off')
    GROUP BY customer_id
) ln ON c.customer_id = ln.customer_id

LEFT JOIN (
    SELECT t.customer_id,
           ROUND(COUNT(*) / 12.0, 1)       AS monthly_tx_count,
           ROUND(SUM(t.amount_zar) / 12.0, 2) AS monthly_tx_value_zar,
           ROUND(AVG(t.amount_zar), 2)     AS avg_tx_value_zar
    FROM fact_transaction t
    WHERE t.transaction_date >= ADD_MONTHS(CURRENT_DATE, -12)
    GROUP BY t.customer_id
) tx ON c.customer_id = tx.customer_id

LEFT JOIN (
    SELECT customer_id,
           COUNT(*)                         AS fraud_alert_count,
           MAX(CASE WHEN status='Open' THEN 1 ELSE 0 END) AS active_fraud_flag
    FROM fact_fraud_alert
    GROUP BY customer_id
) fr ON c.customer_id = fr.customer_id;
