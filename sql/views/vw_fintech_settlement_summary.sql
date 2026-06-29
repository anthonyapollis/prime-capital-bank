-- =============================================================================
-- VW_FINTECH_SETTLEMENT_SUMMARY
-- Monthly fintech partner performance and settlement analytics
-- =============================================================================
CREATE OR REPLACE VIEW gold.vw_fintech_settlement_summary AS
SELECT
    fs.settlement_id,
    fs.settlement_month,
    DATE_FORMAT(fs.settlement_month, 'yyyy-MM')                 AS month_label,
    DATE_FORMAT(fs.settlement_month, 'MMMM yyyy')               AS month_name,

    -- Partner details
    ft.fintech_id,
    ft.fintech_name,
    ft.platform_type,
    ft.fee_rate_pct,
    ft.min_fee_zar,
    ft.settlement_days,
    ft.risk_rating,

    -- Volume metrics
    fs.gross_volume_zar,
    fs.transaction_count,
    ROUND(fs.gross_volume_zar / NULLIF(fs.transaction_count, 0), 2) AS avg_basket_zar,

    -- Fee revenue
    fs.mdr_fees_zar,
    fs.net_settlement_zar,
    ROUND(fs.mdr_fees_zar / NULLIF(fs.gross_volume_zar, 0) * 100, 4) AS effective_fee_rate_pct,

    -- Chargeback metrics
    fs.chargeback_count,
    fs.chargeback_value_zar,
    ROUND(fs.chargeback_count * 100.0 / NULLIF(fs.transaction_count, 0), 4) AS chargeback_rate_pct,

    -- QoQ growth (window functions)
    fs.gross_volume_zar - LAG(fs.gross_volume_zar, 1) OVER (
        PARTITION BY fs.fintech_id ORDER BY fs.settlement_month
    )                                                           AS vol_change_qoq_zar,
    ROUND(
        (fs.gross_volume_zar - LAG(fs.gross_volume_zar, 1) OVER (
            PARTITION BY fs.fintech_id ORDER BY fs.settlement_month
        )) * 100.0 / NULLIF(LAG(fs.gross_volume_zar, 1) OVER (
            PARTITION BY fs.fintech_id ORDER BY fs.settlement_month
        ), 0)
    , 2)                                                        AS vol_growth_pct,

    -- Rolling 3-month average
    ROUND(AVG(fs.gross_volume_zar) OVER (
        PARTITION BY fs.fintech_id
        ORDER BY fs.settlement_month
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 2)                                                       AS rolling_3m_avg_zar,

    -- Rank by volume within month
    RANK() OVER (
        PARTITION BY fs.settlement_month
        ORDER BY fs.gross_volume_zar DESC
    )                                                           AS monthly_volume_rank,

    fs.settlement_status,
    fs.dispute_count

FROM fact_fintech_settlement fs
JOIN dim_fintech ft ON fs.fintech_id = ft.fintech_id;


-- =============================================================================
-- VW_FINTECH_ANNUAL_COMPARISON
-- Year-level fintech partner comparison for executive reporting
-- =============================================================================
CREATE OR REPLACE VIEW gold.vw_fintech_annual_comparison AS
SELECT
    ft.fintech_id,
    ft.fintech_name,
    ft.platform_type,
    ft.fee_rate_pct,
    ft.active_merchants_est,
    ft.annual_volume_zar_bn                                     AS reported_annual_volume_bn,
    SUM(fs.gross_volume_zar) / 1e9                              AS actual_annual_volume_bn,
    SUM(fs.mdr_fees_zar)                                        AS total_fees_earned_zar,
    SUM(fs.transaction_count)                                   AS total_transactions,
    ROUND(SUM(fs.gross_volume_zar) / NULLIF(SUM(fs.transaction_count), 0), 2) AS avg_basket_zar,
    SUM(fs.chargeback_count)                                    AS total_chargebacks,
    ROUND(SUM(fs.chargeback_count) * 100.0 / NULLIF(SUM(fs.transaction_count), 0), 4) AS annual_chargeback_rate,
    MAX(fs.gross_volume_zar)                                    AS peak_month_volume_zar,
    MIN(fs.gross_volume_zar)                                    AS trough_month_volume_zar
FROM fact_fintech_settlement fs
JOIN dim_fintech ft ON fs.fintech_id = ft.fintech_id
GROUP BY
    ft.fintech_id, ft.fintech_name, ft.platform_type,
    ft.fee_rate_pct, ft.active_merchants_est, ft.annual_volume_zar_bn;
