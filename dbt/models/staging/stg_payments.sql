{{
    config(
        materialized = 'view',
        tags         = ['staging', 'payments', 'daily'],
        description  = 'Staged payment records: cleaned, enriched with payment rail classification and settlement lag.'
    )
}}

/*
  stg_payments
  ─────────────────────────────────────────────────────────────────────────────
  Source  : prime_capital.bronze.bronze_payments
  Layer   : Silver (staging)
  Grain   : One row per payment instruction (payment_id)
  Purpose : Clean raw payment data, classify each payment into a payment rail
            (EFT / RTGS / SWIFT / Internal), and compute settlement lag days.
            Feeds int_payment_analytics and mart_treasury_analytics.
  ─────────────────────────────────────────────────────────────────────────────
*/

with

source as (
    select * from {{ source('bronze', 'bronze_payments') }}
),

cast_and_rename as (
    select
        -- Keys
        cast(payment_id             as string)                      as payment_id,
        cast(account_id             as string)                      as account_id,
        cast(customer_id            as string)                      as customer_id,
        cast(initiated_by_user_id   as string)                      as initiated_by_user_id,

        -- Counterparty identifiers
        trim(beneficiary_account_number)                            as beneficiary_account_number,
        trim(beneficiary_name)                                      as beneficiary_name,
        upper(trim(beneficiary_bank_code))                          as beneficiary_bank_code,
        upper(trim(beneficiary_country_code))                       as beneficiary_country_code,

        -- Dates & status
        cast(payment_date           as date)                        as payment_date,
        cast(submission_timestamp   as timestamp)                   as submission_timestamp,
        cast(settlement_timestamp   as timestamp)                   as settlement_timestamp,
        cast(expected_settlement_date as date)                      as expected_settlement_date,
        upper(trim(payment_status))                                 as payment_status,
        -- SUBMITTED / PROCESSING / SETTLED / FAILED / RETURNED / CANCELLED

        -- Amounts
        cast(payment_amount         as decimal(18,2))               as payment_amount_orig,
        upper(trim(currency_code))                                  as currency_code,
        cast(payment_fee            as decimal(10,2))               as payment_fee_orig,

        -- Raw classification fields
        upper(trim(payment_type))                                   as payment_type_raw,
        upper(trim(payment_channel))                                as payment_channel_raw,
        upper(trim(payment_purpose))                                as payment_purpose,
        -- SALARY / SUPPLIER / INTERBANK / PERSONAL / LOAN_REPAYMENT / OTHER

        -- Reference fields
        trim(payment_reference)                                     as payment_reference,
        trim(batch_id)                                              as batch_id,
        trim(clearing_house_ref)                                    as clearing_house_ref,

        -- Failure info
        upper(trim(failure_reason_code))                            as failure_reason_code,
        trim(failure_reason_description)                            as failure_reason_description,

        -- Audit
        cast(created_at             as timestamp)                   as created_at,
        cast(_ingested_at           as timestamp)                   as _ingested_at

    from source
    where payment_id is not null
),

payment_rail_classified as (
    select
        *,

        -- ── Payment rail classification ────────────────────────────────────────
        -- South African payment streams: EFT (ACH), RTGS, SWIFT (cross-border), Internal
        case
            -- Internal: same-bank transfers
            when payment_type_raw in ('INTERNAL', 'INT_TRANSFER', 'OWN_ACCOUNT')   then 'Internal'

            -- RTGS: high-value same-day (typically > R5M or SAMOS)
            when payment_type_raw in ('RTGS', 'SAMOS', 'HIGH_VALUE')               then 'RTGS'

            -- SWIFT: cross-border or foreign currency
            when payment_type_raw in ('SWIFT', 'WIRE', 'CROSS_BORDER', 'FOREX_PAY') then 'SWIFT'
            when beneficiary_country_code not in ('ZA', 'NA', '') and
                 beneficiary_country_code is not null                               then 'SWIFT'

            -- EFT: standard electronic funds transfer (PASA/BankServ)
            when payment_type_raw in ('EFT', 'ACH', 'NAEDO', 'DEBI_CHECK',
                                       'REAL_TIME', 'RTC', 'NRT')                  then 'EFT'

            -- Default to EFT for unknown domestic
            else 'EFT'
        end                                                         as payment_rail,

        -- ── Is cross-border flag ──────────────────────────────────────────────
        case
            when beneficiary_country_code not in ('ZA', '') and
                 beneficiary_country_code is not null               then true
            when payment_type_raw in ('SWIFT', 'WIRE', 'CROSS_BORDER')             then true
            else false
        end                                                         as is_cross_border,

        -- ── Payment failure flag ──────────────────────────────────────────────
        case when payment_status in ('FAILED', 'RETURNED') then true else false end
                                                                    as is_failed

    from cast_and_rename
),

settlement_enriched as (
    select
        *,

        -- ── Settlement lag: actual - submission (business days approximation) ──
        case
            when settlement_timestamp is not null
            then datediff(
                    date(settlement_timestamp),
                    date(submission_timestamp)
                 )
            when payment_status = 'SETTLED' and expected_settlement_date is not null
            then datediff(expected_settlement_date, payment_date)
            else null
        end                                                         as settlement_lag_days,

        -- ── Expected SLA by rail ──────────────────────────────────────────────
        case
            when payment_rail = 'Internal' then 0   -- same-day
            when payment_rail = 'EFT'      then 1   -- next business day
            when payment_rail = 'RTGS'     then 0   -- same-day settlement
            when payment_rail = 'SWIFT'    then 3   -- T+3 typical
            else 2
        end                                                         as expected_settlement_days,

        -- ── SLA breach flag ───────────────────────────────────────────────────
        case
            when settlement_timestamp is null and payment_status != 'SETTLED'  then false
            when datediff(
                    date(coalesce(settlement_timestamp, current_timestamp())),
                    date(submission_timestamp)
                 ) >
                 case
                     when payment_rail = 'Internal' then 0
                     when payment_rail = 'EFT'      then 1
                     when payment_rail = 'RTGS'     then 0
                     when payment_rail = 'SWIFT'    then 3
                     else 2
                 end                                                then true
            else false
        end                                                         as is_sla_breach,

        -- ── ZAR amounts (pass-through; full FX via macro if currency != ZAR) ──
        {{ convert_to_zar('payment_amount_orig', 'currency_code') }} as payment_amount_zar,
        {{ convert_to_zar('payment_fee_orig',    'currency_code') }} as payment_fee_zar

    from payment_rail_classified
),

final as (
    select
        payment_id,
        account_id,
        customer_id,
        initiated_by_user_id,
        beneficiary_account_number,
        beneficiary_name,
        beneficiary_bank_code,
        beneficiary_country_code,
        is_cross_border,
        payment_date,
        submission_timestamp,
        settlement_timestamp,
        expected_settlement_date,
        payment_status,
        is_failed,
        payment_rail,
        payment_type_raw,
        payment_channel_raw,
        payment_purpose,
        payment_amount_orig,
        currency_code,
        payment_amount_zar,
        payment_fee_orig,
        payment_fee_zar,
        settlement_lag_days,
        expected_settlement_days,
        is_sla_breach,
        payment_reference,
        batch_id,
        clearing_house_ref,
        failure_reason_code,
        failure_reason_description,
        created_at,
        _ingested_at,
        current_timestamp()                                         as loaded_at
    from settlement_enriched
)

select * from final
