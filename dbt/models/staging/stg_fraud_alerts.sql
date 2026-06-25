{{
    config(
        materialized = 'view',
        tags         = ['staging', 'fraud', 'compliance', 'daily'],
        description  = 'Staged fraud alerts: type taxonomy, priority scoring, and resolution time.'
    )
}}

/*
  stg_fraud_alerts
  ─────────────────────────────────────────────────────────────────────────────
  Source  : prime_capital.bronze.bronze_fraud_alerts
  Layer   : Silver (staging)
  Grain   : One row per fraud alert (alert_id)
  Purpose : Enrich raw fraud alerts with a standardised fraud_type taxonomy,
            calculate an alert_priority (Low / Medium / High / Critical), and
            derive days_to_resolve. Feeds int_fraud_pattern and
            mart_fraud_intelligence.
  ─────────────────────────────────────────────────────────────────────────────
*/

with

source as (
    select * from {{ source('bronze', 'bronze_fraud_alerts') }}
),

cast_and_rename as (
    select
        -- Keys
        cast(alert_id               as string)                      as alert_id,
        cast(customer_id            as string)                      as customer_id,
        cast(account_id             as string)                      as account_id,
        cast(transaction_id         as string)                      as transaction_id,
        cast(card_id                as string)                      as card_id,
        cast(assigned_investigator  as string)                      as assigned_investigator_id,

        -- Alert descriptors
        upper(trim(alert_type))                                     as alert_type_raw,
        upper(trim(fraud_category))                                 as fraud_category_raw,
        upper(trim(alert_status))                                   as alert_status,
        -- OPEN / INVESTIGATING / CONFIRMED_FRAUD / FALSE_POSITIVE / CLOSED
        upper(trim(detection_source))                               as detection_source,
        -- RULE_ENGINE / ML_MODEL / CUSTOMER_REPORT / STAFF / CORRESPONDENT_BANK

        -- Scores & amounts
        cast(fraud_score            as decimal(5,2))                as fraud_score,     -- 0–100
        cast(alert_amount_zar       as decimal(18,2))               as alert_amount_zar,
        cast(loss_amount_zar        as decimal(18,2))               as confirmed_loss_zar,
        cast(recovered_amount_zar   as decimal(14,2))               as recovered_amount_zar,

        -- Geography
        upper(trim(transaction_country))                            as transaction_country,
        upper(trim(ip_country))                                     as ip_country,
        trim(merchant_name)                                         as merchant_name,
        upper(trim(merchant_category_code))                         as merchant_category_code,

        -- Dates
        cast(alert_raised_timestamp as timestamp)                   as alert_raised_timestamp,
        cast(alert_date             as date)                        as alert_date,
        cast(resolved_timestamp     as timestamp)                   as resolved_timestamp,
        cast(transaction_timestamp  as timestamp)                   as transaction_timestamp,

        -- Outcomes
        upper(trim(resolution_outcome))                             as resolution_outcome,
        -- FRAUD_CONFIRMED / FALSE_POSITIVE / CUSTOMER_ACKNOWLEDGED / UNRESOLVED
        cast(chargeback_raised      as boolean)                     as chargeback_raised,
        cast(police_case_number     as string)                      as police_case_number,

        -- Audit
        cast(created_at             as timestamp)                   as created_at,
        cast(_ingested_at           as timestamp)                   as _ingested_at

    from source
    where alert_id is not null
),

fraud_taxonomy as (
    select
        *,

        -- ── Standardised fraud type taxonomy ──────────────────────────────────
        case
            -- Card-Not-Present (CNP) fraud — online / e-commerce
            when alert_type_raw in ('CNP', 'CARD_NOT_PRESENT', 'ECOM_FRAUD',
                                     'ONLINE_FRAUD')                        then 'Card-Not-Present'

            -- Card-Present fraud — physical POS skimming / cloning
            when alert_type_raw in ('CARD_PRESENT', 'SKIMMING', 'CLONING',
                                     'POS_FRAUD', 'ATM_FRAUD')              then 'Card-Present'

            -- Identity fraud / account takeover
            when alert_type_raw in ('IDENTITY_FRAUD', 'ATO', 'ACCOUNT_TAKEOVER',
                                     'SIM_SWAP', 'PHISHING')               then 'Identity Fraud'

            -- Cheque fraud
            when alert_type_raw in ('CHEQUE_FRAUD', 'FORGED_CHEQUE',
                                     'KITING')                              then 'Cheque Fraud'

            -- Insider fraud / employee-related
            when alert_type_raw in ('INSIDER_FRAUD', 'STAFF_FRAUD',
                                     'EMPLOYEE_COLLUSION')                  then 'Insider Fraud'

            -- First-party fraud (deliberate default / bust-out)
            when alert_type_raw in ('FIRST_PARTY', 'BUST_OUT', 'SYNTHETIC_ID')  then 'First-Party Fraud'

            -- Authorised push payment (APP)
            when alert_type_raw in ('APP_FRAUD', 'VISHING', 'SMISHING',
                                     'ROMANCE_SCAM', 'INVESTMENT_SCAM')    then 'Authorised Push Payment'

            else 'Other'
        end                                                         as fraud_type,

        -- ── Alert channel: was this card-present or card-not-present? ─────────
        case
            when alert_type_raw in ('CNP', 'CARD_NOT_PRESENT', 'ECOM_FRAUD',
                                     'ONLINE_FRAUD', 'PHISHING', 'APP_FRAUD',
                                     'VISHING', 'SMISHING')                then 'Card-Not-Present'
            when alert_type_raw in ('CARD_PRESENT', 'SKIMMING', 'CLONING',
                                     'POS_FRAUD', 'ATM_FRAUD')             then 'Card-Present'
            else 'Non-Card'
        end                                                         as fraud_channel_type

    from cast_and_rename
),

priority_scored as (
    select
        *,

        -- ── Alert priority (Low / Medium / High / Critical) ───────────────────
        -- Logic: combination of fraud_score and alert_amount
        case
            when fraud_score >= 80 or alert_amount_zar >= 500000   then 'Critical'
            when fraud_score >= 60 or alert_amount_zar >= 100000   then 'High'
            when fraud_score >= 40 or alert_amount_zar >= 10000    then 'Medium'
            else                                                         'Low'
        end                                                         as alert_priority,

        -- ── Cross-border flag ─────────────────────────────────────────────────
        case
            when transaction_country not in ('ZA', '') and
                 transaction_country is not null                     then true
            else false
        end                                                         as is_cross_border_transaction,

        -- ── Geo-mismatch flag (IP country differs from card country) ──────────
        case
            when ip_country is not null
             and ip_country != 'ZA'
             and transaction_country = 'ZA'                         then true
            else false
        end                                                         as geo_mismatch_flag

    from fraud_taxonomy
),

resolution_enriched as (
    select
        *,

        -- ── Days to resolve ────────────────────────────────────────────────────
        case
            when resolved_timestamp is not null
            then datediff(date(resolved_timestamp), date(alert_raised_timestamp))
            else null   -- still open
        end                                                         as days_to_resolve,

        -- ── Is confirmed fraud ────────────────────────────────────────────────
        case
            when alert_status = 'CONFIRMED_FRAUD'
              or resolution_outcome = 'FRAUD_CONFIRMED'             then true
            else false
        end                                                         as is_confirmed_fraud,

        -- ── Is false positive ─────────────────────────────────────────────────
        case
            when alert_status = 'FALSE_POSITIVE'
              or resolution_outcome = 'FALSE_POSITIVE'              then true
            else false
        end                                                         as is_false_positive,

        -- ── Net loss after recovery ────────────────────────────────────────────
        round(
            coalesce(confirmed_loss_zar, 0) - coalesce(recovered_amount_zar, 0),
        2)                                                          as net_loss_zar

    from priority_scored
),

final as (
    select
        alert_id,
        customer_id,
        account_id,
        transaction_id,
        card_id,
        assigned_investigator_id,
        fraud_type,
        fraud_category_raw,
        fraud_channel_type,
        alert_type_raw,
        alert_status,
        alert_priority,
        detection_source,
        fraud_score,
        alert_date,
        alert_raised_timestamp,
        resolved_timestamp,
        transaction_timestamp,
        days_to_resolve,
        is_confirmed_fraud,
        is_false_positive,
        is_cross_border_transaction,
        geo_mismatch_flag,
        alert_amount_zar,
        confirmed_loss_zar,
        recovered_amount_zar,
        net_loss_zar,
        resolution_outcome,
        chargeback_raised,
        police_case_number,
        transaction_country,
        ip_country,
        merchant_name,
        merchant_category_code,
        created_at,
        _ingested_at,
        current_timestamp()                                         as loaded_at
    from resolution_enriched
)

select * from final
