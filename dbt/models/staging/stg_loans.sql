{{
    config(
        materialized = 'view',
        tags         = ['staging', 'loans', 'credit_risk', 'daily'],
        description  = 'Staged loan records: delinquency, arrears, LTV, NPL flag, and IAS 39 provisioning buckets.'
    )
}}

/*
  stg_loans
  ─────────────────────────────────────────────────────────────────────────────
  Source  : prime_capital.bronze.bronze_loans
  Layer   : Silver (staging)
  Grain   : One row per loan (loan_id)
  Purpose : Compute delinquency_days, arrears_amount, loan-to-value (LTV),
            NPL flag (>90 DPD), and IAS 39 provisioning rates per bucket.
            Feeds int_loan_health and mart_credit_risk.
  ─────────────────────────────────────────────────────────────────────────────
*/

with

source as (
    select * from {{ source('bronze', 'bronze_loans') }}
),

cast_and_rename as (
    select
        -- Keys
        cast(loan_id                as string)                      as loan_id,
        cast(customer_id            as string)                      as customer_id,
        cast(account_id             as string)                      as account_id,
        cast(branch_id              as string)                      as branch_id,
        cast(relationship_manager   as string)                      as relationship_manager_id,

        -- Loan descriptors
        upper(trim(loan_type))                                      as loan_type,
        -- HOME_LOAN / VEHICLE / PERSONAL / BUSINESS / OVERDRAFT / CREDIT_CARD
        upper(trim(loan_status))                                    as loan_status,
        -- ACTIVE / SETTLED / WRITTEN_OFF / NPL / RESTRUCTURED
        upper(trim(currency_code))                                  as currency_code,
        upper(trim(collateral_type))                                as collateral_type,
        -- PROPERTY / VEHICLE / SURETY / UNSECURED / CESSION

        -- Origination & key dates
        cast(origination_date       as date)                        as origination_date,
        cast(maturity_date          as date)                        as maturity_date,
        cast(last_payment_date      as date)                        as last_payment_date,
        cast(next_payment_date      as date)                        as next_payment_date,
        cast(first_missed_payment_date as date)                     as first_missed_payment_date,

        -- Financial amounts (ZAR)
        cast(original_loan_amount   as decimal(18,2))               as original_loan_amount_zar,
        cast(outstanding_principal  as decimal(18,2))               as outstanding_principal_zar,
        cast(outstanding_interest   as decimal(18,2))               as outstanding_interest_zar,
        cast(outstanding_fees       as decimal(18,2))               as outstanding_fees_zar,
        cast(monthly_instalment     as decimal(12,2))               as monthly_instalment_zar,
        cast(collateral_value       as decimal(18,2))               as collateral_value_zar,

        -- Rates
        cast(interest_rate          as decimal(8,4))                as interest_rate_pct,
        cast(prime_rate_margin       as decimal(8,4))               as prime_rate_margin_pct,

        -- Repayment
        cast(loan_term_months       as int)                         as loan_term_months,
        cast(remaining_term_months  as int)                         as remaining_term_months,
        cast(repayments_made        as int)                         as repayments_made,
        cast(repayments_missed      as int)                         as repayments_missed,

        -- Audit
        cast(created_at             as timestamp)                   as created_at,
        cast(updated_at             as timestamp)                   as updated_at,
        cast(_ingested_at           as timestamp)                   as _ingested_at

    from source
    where loan_id is not null
),

delinquency as (
    select
        *,

        -- ── Days Past Due (DPD) ───────────────────────────────────────────────
        -- Count days from first missed payment to today (0 if no missed payment)
        case
            when first_missed_payment_date is null then 0
            else datediff(current_date(), first_missed_payment_date)
        end                                                         as delinquency_days,

        -- ── Arrears amount = total overdue obligations ─────────────────────────
        -- missed instalments × monthly instalment (simplified; arrears system feeds full detail)
        round(repayments_missed * monthly_instalment_zar, 2)        as arrears_amount_zar,

        -- ── Total exposure at default ─────────────────────────────────────────
        round(
            outstanding_principal_zar + outstanding_interest_zar + outstanding_fees_zar,
        2)                                                          as total_outstanding_zar

    from cast_and_rename
),

classifications as (
    select
        *,

        -- ── Loan-to-Value ratio ───────────────────────────────────────────────
        case
            when collateral_value_zar > 0
            then round(outstanding_principal_zar / collateral_value_zar * 100, 2)
            else null
        end                                                         as ltv_ratio_pct,

        -- ── NPL flag (Non-Performing Loan: >90 DPD per SARB guidelines) ───────
        case
            when delinquency_days > {{ var('npl_threshold_days') }} then true
            else false
        end                                                         as npl_flag,

        -- ── Delinquency bucket ────────────────────────────────────────────────
        case
            when delinquency_days = 0              then 'Current'
            when delinquency_days between 1  and 30  then '1–30 DPD'
            when delinquency_days between 31 and 60  then '31–60 DPD'
            when delinquency_days between 61 and 90  then '61–90 DPD'
            when delinquency_days > 90             then 'NPL (>90 DPD)'
            else 'Unknown'
        end                                                         as delinquency_bucket,

        -- ── IAS 39 provisioning rate (South African banking standard) ─────────
        -- Stage/bucket: 0% = current, 25% = 1-90 DPD, 50% = 91-180, 100% = >180
        case
            when delinquency_days = 0              then 0.00
            when delinquency_days between 1  and 90  then 0.25
            when delinquency_days between 91 and 180 then 0.50
            else 1.00
        end                                                         as ias39_provisioning_rate,

        -- ── IFRS 9 Stage assignment ───────────────────────────────────────────
        case
            when loan_status = 'WRITTEN_OFF'                         then 'Stage 3'
            when delinquency_days > {{ var('stage3_threshold_days') }}  then 'Stage 3'
            when delinquency_days > {{ var('stage2_threshold_days') }}  then 'Stage 2'
            else                                                          'Stage 1'
        end                                                         as ifrs9_stage,

        -- ── Provision amount (IAS 39) ─────────────────────────────────────────
        round(
            total_outstanding_zar *
            case
                when delinquency_days = 0              then 0.00
                when delinquency_days between 1  and 90  then 0.25
                when delinquency_days between 91 and 180 then 0.50
                else 1.00
            end,
        2)                                                          as provision_amount_zar,

        -- Months since origination (for vintage analysis)
        floor(datediff(current_date(), origination_date) / 30.44)  as vintage_month

    from delinquency
),

final as (
    select
        loan_id,
        customer_id,
        account_id,
        branch_id,
        relationship_manager_id,
        loan_type,
        loan_status,
        currency_code,
        collateral_type,
        origination_date,
        maturity_date,
        last_payment_date,
        next_payment_date,
        first_missed_payment_date,
        delinquency_days,
        delinquency_bucket,
        npl_flag,
        ifrs9_stage,
        ias39_provisioning_rate,
        original_loan_amount_zar,
        outstanding_principal_zar,
        outstanding_interest_zar,
        outstanding_fees_zar,
        total_outstanding_zar,
        arrears_amount_zar,
        provision_amount_zar,
        collateral_value_zar,
        ltv_ratio_pct,
        interest_rate_pct,
        prime_rate_margin_pct,
        monthly_instalment_zar,
        loan_term_months,
        remaining_term_months,
        repayments_made,
        repayments_missed,
        vintage_month,
        created_at,
        updated_at,
        _ingested_at,
        current_timestamp()                                         as loaded_at
    from classifications
)

select * from final
