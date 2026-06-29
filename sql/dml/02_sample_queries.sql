-- =============================================================================
-- 02_SAMPLE_QUERIES.SQL
-- Analyst-ready queries for common reporting use cases
-- All queries target the Gold layer views
-- =============================================================================

-- ── 1. TOP 10 CUSTOMERS BY LIFETIME VALUE SCORE ───────────────────────────
SELECT
    customer_id,
    full_name,
    customer_segment,
    province,
    credit_band,
    clv_score,
    total_balance_zar,
    monthly_tx_value_zar,
    tenure_years
FROM gold.vw_customer_360
WHERE is_active = TRUE
ORDER BY clv_score DESC
LIMIT 10;


-- ── 2. FRAUD HOTSPOT BY PROVINCE (LAST 12 MONTHS) ────────────────────────
SELECT
    province,
    total_alerts,
    affected_customers,
    ROUND(total_fraud_value_zar / 1e6, 2)   AS fraud_value_millions_zar,
    avg_risk_score,
    cnp_alerts,
    ato_alerts,
    province_risk_tier,
    pct_of_national_alerts
FROM gold.vw_fraud_province_summary
ORDER BY total_alerts DESC;


-- ── 3. FINTECH PARTNER PERFORMANCE — MONTHLY TREND ───────────────────────
SELECT
    fintech_name,
    month_label,
    ROUND(gross_volume_zar / 1e6, 2)        AS volume_millions_zar,
    transaction_count,
    avg_basket_zar,
    ROUND(mdr_fees_zar / 1e3, 2)            AS fees_thousands_zar,
    effective_fee_rate_pct,
    chargeback_rate_pct,
    vol_growth_pct,
    monthly_volume_rank
FROM gold.vw_fintech_settlement_summary
ORDER BY settlement_month DESC, monthly_volume_rank;


-- ── 4. BRANCH PERFORMANCE TIER SUMMARY ────────────────────────────────────
SELECT
    branch_tier,
    COUNT(*)                                        AS branch_count,
    SUM(total_customers)                            AS total_customers,
    ROUND(SUM(total_deposits_zar) / 1e9, 2)        AS total_deposits_billions,
    ROUND(SUM(total_loan_book_zar) / 1e9, 2)       AS total_loans_billions,
    ROUND(AVG(npl_ratio_pct), 2)                   AS avg_npl_ratio_pct,
    ROUND(AVG(digital_adoption_pct), 1)             AS avg_digital_adoption_pct,
    ROUND(SUM(estimated_nii_zar) / 1e6, 2)         AS total_estimated_nii_millions
FROM gold.vw_branch_performance
WHERE is_active = TRUE
GROUP BY branch_tier
ORDER BY branch_tier;


-- ── 5. HIGH-RISK CUSTOMERS (MULTI-SIGNAL) ────────────────────────────────
SELECT
    customer_id,
    full_name,
    province,
    credit_band,
    fraud_alert_count,
    utilisation_pct,
    clv_score,
    annual_income_zar,
    monthly_tx_value_zar
FROM gold.vw_customer_360
WHERE active_fraud_flag = 1
   OR utilisation_pct > 90
   OR credit_band IN ('Poor', 'Very Poor')
ORDER BY fraud_alert_count DESC, utilisation_pct DESC
LIMIT 50;


-- ── 6. FINTECH FEE REVENUE VS CHARGEBACK COST ────────────────────────────
SELECT
    fintech_name,
    platform_type,
    fee_rate_pct,
    SUM(gross_volume_zar) / 1e9                AS annual_volume_bn,
    SUM(mdr_fees_zar) / 1e6                    AS annual_fees_m,
    SUM(chargeback_value_zar) / 1e3            AS chargeback_cost_k,
    ROUND(SUM(mdr_fees_zar) - SUM(chargeback_value_zar), 2) AS net_revenue_zar,
    ROUND(AVG(chargeback_rate_pct), 4)         AS avg_chargeback_rate_pct
FROM gold.vw_fintech_settlement_summary
GROUP BY fintech_name, platform_type, fee_rate_pct
ORDER BY annual_fees_m DESC;


-- ── 7. MONTH-ON-MONTH FRAUD TREND ─────────────────────────────────────────
SELECT
    DATE_FORMAT(alert_date, 'yyyy-MM')          AS month,
    COUNT(*)                                    AS total_alerts,
    COUNT(DISTINCT customer_id)                 AS unique_customers,
    SUM(amount_zar) / 1e6                       AS fraud_value_millions,
    AVG(risk_score)                             AS avg_risk_score,
    SUM(CASE WHEN status='Resolved' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS resolution_rate_pct
FROM fact_fraud_alert
WHERE alert_date >= ADD_MONTHS(CURRENT_DATE, -18)
GROUP BY DATE_FORMAT(alert_date, 'yyyy-MM')
ORDER BY month DESC;


-- ── 8. REGULATORY CAPITAL ADEQUACY SNAPSHOT ───────────────────────────────
SELECT
    b.province,
    SUM(l.outstanding_balance_zar)                          AS total_exposure_zar,
    SUM(l.outstanding_balance_zar * l.risk_weight_pct / 100) AS risk_weighted_assets_zar,
    SUM(l.provision_zar)                                    AS total_provisions_zar,
    ROUND(
        SUM(l.provision_zar) * 100.0 / NULLIF(SUM(l.outstanding_balance_zar), 0)
    , 2)                                                    AS provision_coverage_pct,
    SUM(CASE WHEN l.loan_status='Non-Performing'
        THEN l.outstanding_balance_zar ELSE 0 END)         AS npl_balance_zar,
    ROUND(
        SUM(CASE WHEN l.loan_status='Non-Performing'
            THEN l.outstanding_balance_zar ELSE 0 END) * 100.0
        / NULLIF(SUM(l.outstanding_balance_zar), 0)
    , 2)                                                    AS npl_ratio_pct
FROM fact_loan l
JOIN dim_account a ON l.account_id = a.account_id
JOIN dim_branch  b ON a.branch_id  = b.branch_id
WHERE l.loan_status NOT IN ('Closed')
GROUP BY b.province
ORDER BY total_exposure_zar DESC;
