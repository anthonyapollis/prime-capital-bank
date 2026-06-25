{{
    config(
        materialized = 'table',
        tags         = ['intermediate', 'fraud', 'compliance', 'daily'],
        description  = 'Fraud pattern analysis: CNP vs CP, online vs branch, velocity indicators per customer/account.'
    )
}}

/*
  int_fraud_pattern
  ─────────────────────────────────────────────────────────────────────────────
  Sources : stg_fraud_alerts, stg_transactions, stg_credit_cards
  Layer   : Intermediate
  Grain   : One row per customer (customer_id), aggregating fraud signals
            across all linked accounts and cards
  Purpose : Identify fraud behaviour patterns: card-not-present vs card-present
            split, online vs branch channel exposure, transaction velocity
            anomalies, and merchant category hot-spots. Feeds
            mart_fraud_intelligence.
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
        is_confirmed_fraud,
        is_false_positive,
        is_cross_border_transaction,
        geo_mismatch_flag,
        alert_amount_zar,
        confirmed_loss_zar,
        recovered_amount_zar,
        net_loss_zar,
        days_to_resolve,
        merchant_name,
        merchant_category_code,
        transaction_country,
        chargeback_raised
    from {{ ref('stg_fraud_alerts') }}
),

transactions as (
    select
        transaction_id,
        account_id,
        customer_id,
        transaction_date,
        transaction_timestamp,
        transaction_type,
        transaction_amount_zar,
        channel_code,
        is_credit,
        is_debit,
        transaction_status,
        narration
    from {{ ref('stg_transactions') }}
    where transaction_date >= date_add(current_date(), -365)
),

-- Credit card transactions (card-specific context)
cards as (
    select
        card_id,
        account_id,
        customer_id,
        card_type,
        card_tier,
        utilisation_rate_pct,
        total_purchases_mtd_zar,
        is_overlimit
    from {{ ref('stg_credit_cards') }}
),

-- ── Enrich fraud alerts with transaction context ──────────────────────────────
fraud_with_txn as (
    select
        f.*,
        t.channel_code                                              as txn_channel_code,
        t.transaction_amount_zar                                    as txn_amount_zar,
        t.transaction_date                                          as txn_date,
        t.transaction_timestamp                                     as txn_timestamp
    from fraud_alerts f
    left join transactions t using (transaction_id)
),

-- ── Customer-level fraud aggregates ───────────────────────────────────────────
customer_fraud_agg as (
    select
        customer_id,
        -- Total alert counts
        count(alert_id)                                             as total_alerts,
        count(case when is_confirmed_fraud = true then 1 end)       as confirmed_fraud_alerts,
        count(case when is_false_positive  = true then 1 end)       as false_positive_alerts,

        -- Channel breakdown
        count(case when fraud_channel_type = 'Card-Not-Present' then 1 end) as cnp_alerts,
        count(case when fraud_channel_type = 'Card-Present'     then 1 end) as cp_alerts,
        count(case when fraud_channel_type = 'Non-Card'         then 1 end) as non_card_alerts,

        -- Transaction channel (where the txn occurred)
        count(case when txn_channel_code = 'INTERNET_BANKING'  then 1 end) as online_alerts,
        count(case when txn_channel_code = 'BRANCH'            then 1 end) as branch_alerts,
        count(case when txn_channel_code = 'ATM'               then 1 end) as atm_alerts,
        count(case when txn_channel_code = 'MOBILE_BANKING'    then 1 end) as mobile_alerts,
        count(case when txn_channel_code = 'POS'               then 1 end) as pos_alerts,

        -- Fraud type distribution
        count(case when fraud_type = 'Card-Not-Present'     then 1 end)    as cnp_fraud_count,
        count(case when fraud_type = 'Card-Present'         then 1 end)    as card_present_fraud_count,
        count(case when fraud_type = 'Identity Fraud'       then 1 end)    as identity_fraud_count,
        count(case when fraud_type = 'Authorised Push Payment' then 1 end) as app_fraud_count,
        count(case when fraud_type = 'Insider Fraud'        then 1 end)    as insider_fraud_count,

        -- Priority distribution
        count(case when alert_priority = 'Critical' then 1 end)            as critical_alerts,
        count(case when alert_priority = 'High'     then 1 end)            as high_alerts,
        count(case when alert_priority = 'Medium'   then 1 end)            as medium_alerts,
        count(case when alert_priority = 'Low'      then 1 end)            as low_alerts,

        -- Cross-border fraud
        count(case when is_cross_border_transaction = true then 1 end)     as cross_border_alerts,
        count(case when geo_mismatch_flag          = true then 1 end)      as geo_mismatch_alerts,

        -- Financial impact
        sum(coalesce(alert_amount_zar,    0))                       as total_alert_value_zar,
        sum(coalesce(confirmed_loss_zar,  0))                       as total_confirmed_loss_zar,
        sum(coalesce(recovered_amount_zar,0))                       as total_recovered_zar,
        sum(coalesce(net_loss_zar,        0))                       as total_net_loss_zar,
        count(case when chargeback_raised = true then 1 end)        as chargeback_count,

        -- Score metrics
        max(fraud_score)                                            as max_fraud_score,
        avg(fraud_score)                                            as avg_fraud_score,

        -- Resolution
        avg(case when days_to_resolve is not null then days_to_resolve end)
                                                                    as avg_days_to_resolve,

        -- Detection source mix
        count(case when detection_source = 'ML_MODEL'           then 1 end) as ml_detected,
        count(case when detection_source = 'RULE_ENGINE'        then 1 end) as rule_detected,
        count(case when detection_source = 'CUSTOMER_REPORT'    then 1 end) as customer_reported,

        -- Date range
        min(alert_date)                                             as first_alert_date,
        max(alert_date)                                             as last_alert_date

    from fraud_with_txn
    group by customer_id
),

-- ── Transaction velocity: detect unusual burst patterns ───────────────────────
-- Flag customers with > 10 transactions in any 24-hour window (rolling)
velocity_alerts as (
    select
        customer_id,
        transaction_date,
        count(transaction_id)                                       as txn_count_day,
        sum(transaction_amount_zar)                                 as txn_value_day
    from transactions
    where is_debit = true
    group by customer_id, transaction_date
),

velocity_agg as (
    select
        customer_id,
        max(txn_count_day)                                          as max_daily_txn_count,
        max(txn_value_day)                                          as max_daily_txn_value_zar,
        count(case when txn_count_day > 10 then 1 end)              as high_velocity_days,
        -- Average daily transactions (proxy for normal behaviour)
        avg(txn_count_day)                                          as avg_daily_txn_count
    from velocity_alerts
    group by customer_id
),

-- ── Join customer fraud + velocity ────────────────────────────────────────────
combined as (
    select
        cf.*,
        coalesce(v.max_daily_txn_count,    0)                       as max_daily_txn_count,
        coalesce(v.max_daily_txn_value_zar,0)                       as max_daily_txn_value_zar,
        coalesce(v.high_velocity_days,     0)                       as high_velocity_days,
        coalesce(v.avg_daily_txn_count,    0)                       as avg_daily_txn_count
    from customer_fraud_agg cf
    left join velocity_agg v using (customer_id)
),

final as (
    select
        customer_id,
        total_alerts,
        confirmed_fraud_alerts,
        false_positive_alerts,
        -- False positive rate
        case
            when total_alerts > 0
            then round(false_positive_alerts / total_alerts * 100, 2)
            else 0
        end                                                         as false_positive_rate_pct,

        -- CNP vs CP split
        cnp_alerts,
        cp_alerts,
        non_card_alerts,
        case
            when cnp_alerts + cp_alerts > 0
            then round(cnp_alerts / (cnp_alerts + cp_alerts) * 100, 2)
            else null
        end                                                         as cnp_pct,

        -- Online vs branch
        online_alerts,
        branch_alerts,
        atm_alerts,
        mobile_alerts,
        pos_alerts,

        -- Fraud types
        cnp_fraud_count,
        card_present_fraud_count,
        identity_fraud_count,
        app_fraud_count,
        insider_fraud_count,

        -- Priority
        critical_alerts,
        high_alerts,
        medium_alerts,
        low_alerts,

        -- Cross-border
        cross_border_alerts,
        geo_mismatch_alerts,

        -- Financials
        total_alert_value_zar,
        total_confirmed_loss_zar,
        total_recovered_zar,
        total_net_loss_zar,
        chargeback_count,

        -- Scores
        max_fraud_score,
        avg_fraud_score,
        avg_days_to_resolve,

        -- Detection
        ml_detected,
        rule_detected,
        customer_reported,

        -- Velocity
        max_daily_txn_count,
        max_daily_txn_value_zar,
        high_velocity_days,
        avg_daily_txn_count,
        -- Velocity anomaly flag
        case
            when max_daily_txn_count > avg_daily_txn_count * 5 then true
            else false
        end                                                         as velocity_anomaly_flag,

        first_alert_date,
        last_alert_date,
        current_timestamp()                                         as loaded_at
    from combined
)

select * from final
