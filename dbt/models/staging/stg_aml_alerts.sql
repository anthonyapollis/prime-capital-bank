{{
    config(
        materialized = 'view',
        tags         = ['staging', 'aml', 'compliance', 'daily'],
        description  = 'Staged AML alerts: risk scoring, SAR candidate flagging, and typology classification.'
    )
}}

/*
  stg_aml_alerts
  ─────────────────────────────────────────────────────────────────────────────
  Source  : prime_capital.bronze.bronze_aml_alerts
  Layer   : Silver (staging)
  Grain   : One row per AML alert (aml_alert_id)
  Purpose : Risk-score each alert using a composite model, flag SAR (Suspicious
            Activity Report) candidates per FICA s28 thresholds, and classify
            the typology (structuring, layering, placement, etc.).
  ─────────────────────────────────────────────────────────────────────────────
*/

with

source as (
    select * from {{ source('bronze', 'bronze_aml_alerts') }}
),

cast_and_rename as (
    select
        -- Keys
        cast(aml_alert_id           as string)                      as aml_alert_id,
        cast(customer_id            as string)                      as customer_id,
        cast(account_id             as string)                      as account_id,
        cast(assigned_analyst       as string)                      as assigned_analyst_id,

        -- Descriptors
        upper(trim(alert_type))                                     as alert_type_raw,
        upper(trim(alert_status))                                   as alert_status,
        -- OPEN / UNDER_REVIEW / ESCALATED / SAR_FILED / CLOSED / FALSE_POSITIVE
        upper(trim(alert_source))                                   as alert_source,
        -- TRANSACTION_MONITORING / NAME_SCREENING / PEP_MATCH / WATCHLIST / MANUAL

        -- Scores
        cast(risk_score             as decimal(5,2))                as risk_score_raw,  -- 0–100
        cast(transaction_count_flagged as int)                      as transaction_count_flagged,
        cast(total_amount_flagged   as decimal(18,2))               as total_amount_flagged_zar,
        cast(highest_single_amount  as decimal(18,2))               as highest_single_amount_zar,

        -- Customer profile at time of alert
        upper(trim(customer_risk_category))                         as customer_risk_category_raw,
        -- LOW / MEDIUM / HIGH / VERY_HIGH
        cast(is_pep                 as boolean)                     as customer_is_pep,
        cast(is_sanctioned          as boolean)                     as customer_is_sanctioned,
        upper(trim(nationality))                                    as customer_nationality,
        upper(trim(business_type))                                  as business_type,

        -- Watchlist / screening hits
        upper(trim(watchlist_match_type))                           as watchlist_match_type,
        -- EXACT / FUZZY / PHONETIC / NO_MATCH
        cast(watchlist_match_score  as decimal(5,2))                as watchlist_match_score,
        trim(watchlist_list_name)                                   as watchlist_list_name,

        -- Dates
        cast(alert_date             as date)                        as alert_date,
        cast(alert_raised_timestamp as timestamp)                   as alert_raised_timestamp,
        cast(review_completed_date  as date)                        as review_completed_date,
        cast(sar_filed_date         as date)                        as sar_filed_date,
        cast(reporting_period_start as date)                        as reporting_period_start,
        cast(reporting_period_end   as date)                        as reporting_period_end,

        -- Outcome
        upper(trim(disposition))                                    as disposition,
        -- SAR_FILED / CLOSED_NO_ACTION / ESCALATED / MONITORING_ENHANCED
        trim(sar_reference_number)                                  as sar_reference_number,
        cast(str_filed              as boolean)                     as str_filed,  -- Suspicious Tx Report

        -- Geographic
        upper(trim(primary_country_exposure))                       as primary_country_exposure,
        cast(high_risk_country_flag as boolean)                     as high_risk_country_flag,

        -- Audit
        cast(created_at             as timestamp)                   as created_at,
        cast(updated_at             as timestamp)                   as updated_at,
        cast(_ingested_at           as timestamp)                   as _ingested_at

    from source
    where aml_alert_id is not null
),

typology_classified as (
    select
        *,

        -- ── AML Typology classification (FATF typologies) ────────────────────
        case
            when alert_type_raw in ('STRUCTURING', 'SMURFING', 'BELOW_THRESHOLD')  then 'Structuring'
            when alert_type_raw in ('LAYERING', 'ROUND_TRIP', 'PASS_THROUGH')      then 'Layering'
            when alert_type_raw in ('PLACEMENT', 'CASH_INTENSIVE', 'ATM_CYCLING')  then 'Placement'
            when alert_type_raw in ('INTEGRATION', 'SHELL_COMPANY', 'REAL_ESTATE') then 'Integration'
            when alert_type_raw in ('TRADE_MISINVOICING', 'TBML')                 then 'Trade-Based ML'
            when alert_type_raw in ('PEP_ACTIVITY', 'CORRUPTION', 'BRIBERY')      then 'Corruption/PEP'
            when alert_type_raw in ('TERRORIST_FINANCING', 'TF')                  then 'Terrorist Financing'
            when alert_type_raw in ('SANCTIONS_EVASION', 'SDN_MATCH')             then 'Sanctions Evasion'
            when alert_type_raw in ('CYBER_CRIME', 'RANSOMWARE', 'DARKNET')       then 'Cyber Crime Proceeds'
            else 'Unknown/Other'
        end                                                         as aml_typology

    from cast_and_rename
),

risk_scored as (
    select
        *,

        -- ── Composite AML risk score (0–100) ─────────────────────────────────
        -- Weighted: base score 40%, PEP/sanctions 25%, country risk 15%, amount 20%
        least(100,
            round(
                (risk_score_raw * 0.40)
                + (case when customer_is_pep       then 20 else 0 end)
                + (case when customer_is_sanctioned then 25 else 0 end)
                + (case when high_risk_country_flag then 15 else 0 end)
                + (case
                    when total_amount_flagged_zar >= 1000000 then 20
                    when total_amount_flagged_zar >= 100000  then 10
                    when total_amount_flagged_zar >= 10000   then 5
                    else 0
                  end)
                + (case when watchlist_match_type = 'EXACT' then 25 else 0 end)
            , 2)
        )                                                           as composite_risk_score,

        -- ── Risk band ─────────────────────────────────────────────────────────
        case
            when customer_risk_category_raw = 'VERY_HIGH'          then 'Very High'
            when customer_risk_category_raw = 'HIGH'                then 'High'
            when customer_risk_category_raw = 'MEDIUM'              then 'Medium'
            else                                                          'Low'
        end                                                         as customer_risk_band

    from typology_classified
),

sar_flagged as (
    select
        *,

        -- ── SAR candidate flag (FICA s28 — file if reasonable grounds exist) ──
        -- Trigger conditions per PCB internal AML policy:
        case
            when customer_is_sanctioned                             then true
            when watchlist_match_type = 'EXACT'                    then true
            when composite_risk_score >= 75                        then true
            when aml_typology = 'Terrorist Financing'              then true
            when total_amount_flagged_zar >= 250000
             and composite_risk_score   >= 50                      then true
            when high_risk_country_flag
             and total_amount_flagged_zar >= 100000                 then true
            else false
        end                                                         as sar_candidate_flag,

        -- ── Escalation level ──────────────────────────────────────────────────
        case
            when customer_is_sanctioned
              or aml_typology = 'Terrorist Financing'              then 'Immediate Escalation'
            when composite_risk_score >= 75                        then 'Senior Review Required'
            when composite_risk_score >= 50                        then 'Enhanced Due Diligence'
            else                                                        'Standard Review'
        end                                                         as escalation_level,

        -- ── Days to resolution ────────────────────────────────────────────────
        case
            when review_completed_date is not null
            then datediff(review_completed_date, alert_date)
            else null
        end                                                         as days_to_resolve

    from risk_scored
),

final as (
    select
        aml_alert_id,
        customer_id,
        account_id,
        assigned_analyst_id,
        alert_type_raw,
        aml_typology,
        alert_status,
        alert_source,
        risk_score_raw,
        composite_risk_score,
        customer_risk_band,
        customer_risk_category_raw,
        customer_is_pep,
        customer_is_sanctioned,
        customer_nationality,
        business_type,
        high_risk_country_flag,
        primary_country_exposure,
        watchlist_match_type,
        watchlist_match_score,
        watchlist_list_name,
        transaction_count_flagged,
        total_amount_flagged_zar,
        highest_single_amount_zar,
        sar_candidate_flag,
        escalation_level,
        alert_date,
        alert_raised_timestamp,
        review_completed_date,
        days_to_resolve,
        sar_filed_date,
        sar_reference_number,
        str_filed,
        disposition,
        reporting_period_start,
        reporting_period_end,
        created_at,
        updated_at,
        _ingested_at,
        current_timestamp()                                         as loaded_at
    from sar_flagged
)

select * from final
