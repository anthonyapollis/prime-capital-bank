-- =============================================================================
-- VW_FRAUD_DASHBOARD
-- Real-time fraud intelligence view for analysts and the BI layer
-- =============================================================================
CREATE OR REPLACE VIEW gold.vw_fraud_dashboard AS
SELECT
    fa.alert_id,
    fa.alert_date,
    fa.alert_type,
    fa.fraud_category,
    fa.risk_score,
    CASE
        WHEN fa.risk_score >= 0.80 THEN 'Critical'
        WHEN fa.risk_score >= 0.60 THEN 'High'
        WHEN fa.risk_score >= 0.40 THEN 'Medium'
        ELSE 'Low'
    END                                         AS risk_tier,
    fa.amount_zar,
    fa.status,
    fa.investigation_status,
    fa.resolution_date,
    DATEDIFF(DAY, fa.alert_date, COALESCE(fa.resolution_date, CURRENT_DATE)) AS days_open,

    -- Customer context
    c.customer_id,
    c.first_name || ' ' || c.last_name         AS customer_name,
    c.customer_segment,
    c.province,
    c.city,
    c.credit_score,

    -- Transaction context
    t.transaction_id,
    t.transaction_type,
    t.channel,
    t.amount_zar                                AS tx_amount_zar,
    t.merchant_name,
    t.merchant_category,

    -- Merchant context
    m.merchant_id,
    m.merchant_name                             AS acquiring_merchant,
    m.merchant_category_code                    AS mcc,
    m.province                                  AS merchant_province,

    -- ML model output
    fa.ml_model_version,
    fa.ml_confidence_score,
    fa.feature_flags,

    -- Velocity indicators (30-day window)
    vel.alert_count_30d,
    vel.total_fraud_value_30d,
    vel.avg_risk_score_30d

FROM fact_fraud_alert fa
JOIN dim_customer     c  ON fa.customer_id    = c.customer_id
LEFT JOIN fact_transaction t  ON fa.transaction_id = t.transaction_id
LEFT JOIN dim_merchant     m  ON t.merchant_id     = m.merchant_id

LEFT JOIN (
    SELECT
        customer_id,
        COUNT(*)            AS alert_count_30d,
        SUM(amount_zar)     AS total_fraud_value_30d,
        AVG(risk_score)     AS avg_risk_score_30d
    FROM fact_fraud_alert
    WHERE alert_date >= ADD_MONTHS(CURRENT_DATE, -1)
    GROUP BY customer_id
) vel ON fa.customer_id = vel.customer_id;


-- =============================================================================
-- VW_FRAUD_PROVINCE_SUMMARY
-- Province-level fraud heatmap for the merchant intelligence map
-- =============================================================================
CREATE OR REPLACE VIEW gold.vw_fraud_province_summary AS
SELECT
    c.province,
    COUNT(DISTINCT fa.alert_id)                                 AS total_alerts,
    COUNT(DISTINCT fa.customer_id)                              AS affected_customers,
    SUM(fa.amount_zar)                                          AS total_fraud_value_zar,
    AVG(fa.risk_score)                                          AS avg_risk_score,
    COUNT(DISTINCT CASE WHEN fa.fraud_category='Card Not Present' THEN fa.alert_id END) AS cnp_alerts,
    COUNT(DISTINCT CASE WHEN fa.fraud_category='Account Takeover' THEN fa.alert_id END) AS ato_alerts,
    COUNT(DISTINCT CASE WHEN fa.fraud_category='Identity Fraud'   THEN fa.alert_id END) AS identity_alerts,
    COUNT(DISTINCT CASE WHEN fa.fraud_category='ATM Skimming'     THEN fa.alert_id END) AS atm_alerts,
    COUNT(DISTINCT CASE WHEN fa.fraud_category='Phishing'         THEN fa.alert_id END) AS phishing_alerts,
    ROUND(
        COUNT(DISTINCT fa.alert_id) * 100.0
        / NULLIF(SUM(COUNT(DISTINCT fa.alert_id)) OVER (), 0)
    , 2)                                                        AS pct_of_national_alerts,
    CASE
        WHEN AVG(fa.risk_score) >= 0.70 THEN 'Critical'
        WHEN AVG(fa.risk_score) >= 0.50 THEN 'High'
        WHEN AVG(fa.risk_score) >= 0.35 THEN 'Medium'
        ELSE 'Low'
    END                                                         AS province_risk_tier
FROM fact_fraud_alert fa
JOIN dim_customer c ON fa.customer_id = c.customer_id
WHERE fa.alert_date >= ADD_MONTHS(CURRENT_DATE, -12)
GROUP BY c.province;
