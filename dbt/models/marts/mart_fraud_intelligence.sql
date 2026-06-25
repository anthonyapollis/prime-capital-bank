{{
    config(
        materialized = 'table',
        tags         = ['mart', 'fraud', 'aml', 'compliance', 'daily'],
        description  = 'Fraud analytics mart: fraud rate by channel/product/geography, losses, recovery, model performance.'
    )
}}

/*
  mart_fraud_intelligence
  ─────────────────────────────────────────────────────────────────────────────
  Sources : int_fraud_pattern, stg_fraud_alerts, stg_aml_alerts, stg_transactions
  Layer   : Gold (mart)
  Grain   : One row per fraud alert (alert_id) with enriched analytics fields,
            plus portfolio-level window aggregates
  Purpose : Powers the Fraud Intelligence Dashboard and FICA compliance
            reporting. Includes: fraud rate by channel/product/geography,
            confirmed loss and recovery amounts, false positive rate, and
            model-level precision/recall proxies.
  ─────────────────────────────────────────────────────────────────────────────
*/

with

fraud_alerts as (
    select
        alert_id,
        customer_id,
        account_id,
        transaction_id,
        card_id,
        fraud_type,
        fraud_channel_type,
        alert_type_raw,
        alert_priority,
        alert_status,
        detection_source,
        fraud_score,
        alert_date,
        alert_raised_timestamp,
        resolved_timestamp,
        is_confirmed_fraud,
        is_false_positive,
        is_cross_border_transaction,
        geo_mismatch_flag,
        alert_amount_zar,
        confirmed_loss_zar,
        recovered_amount_zar,
        net_loss_zar,
        days_to_resolve,
        chargeback_raised,
        merchant_name,
        merchant_category_code,
        transaction_country
    from {{ ref('stg_fraud_alerts') }}
),

fraud_patterns as (
    select
        customer_id,
        max_fraud_score,
        avg_fraud_score,
        high_velocity_days,
        velocity_anomaly_flag,
        cnp_pct,
        false_positive_rate_pct,
        ml_detected,
        rule_detected,
        customer_reported
    from {{ ref('int_fraud_pattern') }}
),

-- Transaction context for each alert
transactions as (
    select
        transaction_id,
        transaction_date,
        channel_code,
        transaction_amount_zar,
        transaction_type
    from {{ ref('stg_transactions') }}
),

-- AML cross-reference: flag alerts that also have an AML alert for same customer
aml_flags as (
    select
        customer_id,
        max(case when sar_candidate_flag then 1 else 0 end)         as customer_has_sar,
        max(composite_risk_score)                                    as customer_aml_score
    from {{ ref('stg_aml_alerts') }}
    group by customer_id
),

-- Enrich fraud alert with transaction and pattern data
enriched as (
    select
        f.*,
        t.channel_code                                              as transaction_channel,
        t.transaction_date,
        coalesce(fp.velocity_anomaly_flag, false)                   as velocity_anomaly_flag,
        coalesce(fp.high_velocity_days, 0)                          as customer_high_velocity_days,
        coalesce(fp.false_positive_rate_pct, 0)                     as customer_fpr_pct,
        coalesce(am.customer_has_sar, 0)                            as customer_has_sar,
        coalesce(am.customer_aml_score, 0)                          as customer_aml_score

    from fraud_alerts f
    left join transactions  t  using (transaction_id)
    left join fraud_patterns fp using (customer_id)
    left join aml_flags      am using (customer_id)
),

-- ── Portfolio-level window aggregates ─────────────────────────────────────────
portfolio as (
    select
        *,

        -- Total portfolio fraud metrics (used for rate calculations)
        count(alert_id)
            over ()                                                 as portfolio_total_alerts,
        count(case when is_confirmed_fraud then alert_id end)
            over ()                                                 as portfolio_confirmed_fraud,
        sum(coalesce(net_loss_zar, 0))
            over ()                                                 as portfolio_total_loss_zar,
        sum(coalesce(recovered_amount_zar, 0))
            over ()                                                 as portfolio_total_recovered_zar,

        -- Fraud rate by channel (channel-level)
        count(alert_id)
            over (partition by transaction_channel)                 as channel_total_alerts,
        count(case when is_confirmed_fraud then alert_id end)
            over (partition by transaction_channel)                 as channel_confirmed_fraud,

        -- Fraud rate by fraud type
        count(alert_id)
            over (partition by fraud_type)                          as type_total_alerts,
        count(case when is_confirmed_fraud then alert_id end)
            over (partition by fraud_type)                          as type_confirmed_fraud,
        sum(coalesce(net_loss_zar, 0))
            over (partition by fraud_type)                          as type_total_loss_zar,

        -- Geography: country-level
        count(alert_id)
            over (partition by transaction_country)                 as country_total_alerts,
        sum(coalesce(net_loss_zar, 0))
            over (partition by transaction_country)                 as country_total_loss_zar,

        -- Monthly trend
        count(alert_id)
            over (partition by date_trunc('month', alert_date))     as monthly_alert_count,
        sum(coalesce(net_loss_zar, 0))
            over (partition by date_trunc('month', alert_date))     as monthly_loss_zar

    from enriched
),

final as (
    select
        -- Alert identifiers
        alert_id,
        customer_id,
        account_id,
        transaction_id,
        card_id,

        -- Classification
        fraud_type,
        fraud_channel_type,
        transaction_channel,
        alert_type_raw,
        alert_priority,
        alert_status,
        detection_source,
        is_confirmed_fraud,
        is_false_positive,
        is_cross_border_transaction,
        geo_mismatch_flag,
        velocity_anomaly_flag,
        customer_has_sar,

        -- Scores
        fraud_score,
        customer_aml_score,
        customer_fpr_pct,

        -- Timeline
        alert_date,
        transaction_date,
        date_trunc('month', alert_date)                             as alert_month,
        alert_raised_timestamp,
        resolved_timestamp,
        days_to_resolve,

        -- Geography
        transaction_country,

        -- Merchant
        merchant_name,
        merchant_category_code,

        -- Financials
        alert_amount_zar,
        confirmed_loss_zar,
        recovered_amount_zar,
        net_loss_zar,
        chargeback_raised,

        -- Fraud rate metrics (channel level)
        channel_total_alerts,
        channel_confirmed_fraud,
        round(channel_confirmed_fraud / nullif(channel_total_alerts, 0) * 100, 4)
                                                                    as channel_fraud_rate_pct,

        -- Fraud rate metrics (type level)
        type_total_alerts,
        type_confirmed_fraud,
        type_total_loss_zar,
        round(type_confirmed_fraud / nullif(type_total_alerts, 0) * 100, 4)
                                                                    as type_fraud_rate_pct,

        -- Geography metrics
        country_total_alerts,
        country_total_loss_zar,

        -- Portfolio metrics
        portfolio_total_alerts,
        portfolio_confirmed_fraud,
        round(portfolio_confirmed_fraud / nullif(portfolio_total_alerts, 0) * 100, 4)
                                                                    as portfolio_fraud_rate_pct,
        portfolio_total_loss_zar,
        portfolio_total_recovered_zar,
        round(portfolio_total_recovered_zar / nullif(portfolio_total_loss_zar, 0) * 100, 2)
                                                                    as portfolio_recovery_rate_pct,

        -- Monthly
        monthly_alert_count,
        monthly_loss_zar,

        -- Model performance proxies
        -- Precision = TP / (TP + FP) for ML_MODEL detections
        count(case when detection_source = 'ML_MODEL' and is_confirmed_fraud then 1 end)
            over () * 1.0
            / nullif(count(case when detection_source = 'ML_MODEL' then 1 end) over (), 0)
                                                                    as ml_model_precision,

        -- Recall = TP / (TP + FN) — approximated as confirmed fraud caught by ML vs total confirmed
        count(case when detection_source = 'ML_MODEL' and is_confirmed_fraud then 1 end)
            over () * 1.0
            / nullif(count(case when is_confirmed_fraud then 1 end) over (), 0)
                                                                    as ml_model_recall,

        current_timestamp()                                         as loaded_at

    from portfolio
)

select * from final
