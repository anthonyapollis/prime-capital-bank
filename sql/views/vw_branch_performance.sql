-- =============================================================================
-- VW_BRANCH_PERFORMANCE
-- Branch-level KPIs: revenue, deposits, loans, fraud exposure, NPS
-- =============================================================================
CREATE OR REPLACE VIEW gold.vw_branch_performance AS
WITH branch_txn AS (
    SELECT
        a.branch_id,
        COUNT(t.transaction_id)         AS total_transactions,
        SUM(t.amount_zar)               AS total_tx_value_zar,
        AVG(t.amount_zar)               AS avg_tx_value_zar,
        SUM(CASE WHEN t.channel='Branch' THEN 1 ELSE 0 END) AS branch_channel_txns,
        SUM(CASE WHEN t.channel='Digital' THEN 1 ELSE 0 END) AS digital_channel_txns
    FROM fact_transaction t
    JOIN dim_account a ON t.account_id = a.account_id
    WHERE t.transaction_date >= ADD_MONTHS(CURRENT_DATE, -12)
    GROUP BY a.branch_id
),
branch_deposits AS (
    SELECT
        a.branch_id,
        COUNT(DISTINCT a.account_id)    AS total_accounts,
        SUM(a.current_balance_zar)      AS total_deposits_zar,
        AVG(a.current_balance_zar)      AS avg_account_balance_zar,
        COUNT(DISTINCT a.customer_id)   AS total_customers
    FROM dim_account a
    WHERE a.account_status = 'Active'
    GROUP BY a.branch_id
),
branch_loans AS (
    SELECT
        a.branch_id,
        COUNT(l.loan_id)                AS total_loans,
        SUM(l.approved_amount_zar)      AS total_loan_book_zar,
        SUM(l.outstanding_balance_zar)  AS outstanding_balance_zar,
        AVG(l.interest_rate_pct)        AS avg_interest_rate,
        SUM(CASE WHEN l.loan_status = 'Non-Performing' THEN l.outstanding_balance_zar ELSE 0 END) AS npl_balance_zar,
        ROUND(
            SUM(CASE WHEN l.loan_status = 'Non-Performing' THEN l.outstanding_balance_zar ELSE 0 END)
            / NULLIF(SUM(l.outstanding_balance_zar), 0) * 100
        , 2)                            AS npl_ratio_pct
    FROM fact_loan l
    JOIN dim_account a ON l.account_id = a.account_id
    GROUP BY a.branch_id
),
branch_fraud AS (
    SELECT
        a.branch_id,
        COUNT(fa.alert_id)              AS fraud_alerts,
        SUM(fa.amount_zar)              AS fraud_exposure_zar,
        AVG(fa.risk_score)              AS avg_risk_score
    FROM fact_fraud_alert fa
    JOIN dim_account a ON fa.account_id = a.account_id
    WHERE fa.alert_date >= ADD_MONTHS(CURRENT_DATE, -12)
    GROUP BY a.branch_id
)
SELECT
    b.branch_id,
    b.branch_name,
    b.branch_type,
    b.province,
    b.city,
    b.region,
    b.is_active,
    b.opening_date,

    -- Deposit metrics
    COALESCE(bd.total_customers, 0)         AS total_customers,
    COALESCE(bd.total_accounts, 0)          AS total_accounts,
    COALESCE(bd.total_deposits_zar, 0)      AS total_deposits_zar,
    COALESCE(bd.avg_account_balance_zar, 0) AS avg_account_balance_zar,

    -- Transaction metrics
    COALESCE(bt.total_transactions, 0)      AS annual_transactions,
    COALESCE(bt.total_tx_value_zar, 0)      AS annual_tx_value_zar,
    COALESCE(bt.avg_tx_value_zar, 0)        AS avg_tx_value_zar,
    ROUND(COALESCE(bt.digital_channel_txns, 0) * 100.0
        / NULLIF(COALESCE(bt.total_transactions, 0), 0), 1) AS digital_adoption_pct,

    -- Loan metrics
    COALESCE(bl.total_loans, 0)             AS total_loans,
    COALESCE(bl.total_loan_book_zar, 0)     AS total_loan_book_zar,
    COALESCE(bl.outstanding_balance_zar, 0) AS outstanding_loan_balance_zar,
    COALESCE(bl.avg_interest_rate, 0)       AS avg_interest_rate_pct,
    COALESCE(bl.npl_ratio_pct, 0)           AS npl_ratio_pct,

    -- Revenue proxy (NII approximation)
    ROUND(
        COALESCE(bl.outstanding_balance_zar, 0) * COALESCE(bl.avg_interest_rate, 0) / 100
        - COALESCE(bd.total_deposits_zar, 0) * 0.045
    , 0)                                    AS estimated_nii_zar,

    -- Fraud metrics
    COALESCE(bf.fraud_alerts, 0)            AS fraud_alerts_12m,
    COALESCE(bf.fraud_exposure_zar, 0)      AS fraud_exposure_zar,
    COALESCE(bf.avg_risk_score, 0)          AS avg_fraud_risk_score,

    -- Performance tier
    CASE
        WHEN COALESCE(bd.total_deposits_zar, 0) >= 500000000
          AND COALESCE(bl.npl_ratio_pct, 0) < 3.0    THEN 'Tier 1 — Flagship'
        WHEN COALESCE(bd.total_deposits_zar, 0) >= 200000000 THEN 'Tier 2 — Full Service'
        WHEN COALESCE(bd.total_deposits_zar, 0) >= 50000000  THEN 'Tier 3 — Standard'
        ELSE 'Tier 4 — Micro Branch'
    END                                     AS branch_tier

FROM dim_branch b
LEFT JOIN branch_deposits  bd ON b.branch_id = bd.branch_id
LEFT JOIN branch_txn       bt ON b.branch_id = bt.branch_id
LEFT JOIN branch_loans     bl ON b.branch_id = bl.branch_id
LEFT JOIN branch_fraud     bf ON b.branch_id = bf.branch_id;
