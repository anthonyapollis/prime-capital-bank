{{
    config(
        materialized = 'view',
        tags         = ['staging', 'accounts', 'daily'],
        description  = 'Staged account records: cast, enriched with balance bands, age, and dormancy flag.'
    )
}}

/*
  stg_accounts
  ─────────────────────────────────────────────────────────────────────────────
  Source  : prime_capital.bronze.bronze_accounts
  Layer   : Silver (staging)
  Grain   : One row per account (account_id)
  Purpose : Clean and enrich account master data. Derives account_age_days,
            balance_band, and flags dormant accounts (no transaction in 90 days).
  ─────────────────────────────────────────────────────────────────────────────
*/

with

source as (
    select * from {{ source('bronze', 'bronze_accounts') }}
),

cast_and_rename as (
    select
        -- Keys
        cast(account_id         as string)                          as account_id,
        cast(customer_id        as string)                          as customer_id,
        cast(branch_id          as string)                          as branch_id,

        -- Account descriptors
        upper(trim(account_type))                                   as account_type,
        -- e.g. CHEQUE / SAVINGS / MONEY_MARKET / FIXED_DEPOSIT / NOTICE
        upper(trim(account_status))                                 as account_status,
        -- ACTIVE / CLOSED / FROZEN / DORMANT
        upper(trim(currency_code))                                  as currency_code,
        upper(trim(product_code))                                   as product_code,

        -- Dates
        cast(account_open_date  as date)                            as account_open_date,
        cast(account_close_date as date)                            as account_close_date,
        cast(last_transaction_date as date)                         as last_transaction_date,
        cast(maturity_date      as date)                            as maturity_date,

        -- Balances (ZAR unless currency_code differs — conversion in stg_transactions)
        cast(current_balance    as decimal(18,2))                   as current_balance,
        cast(available_balance  as decimal(18,2))                   as available_balance,
        cast(overdraft_limit    as decimal(18,2))                   as overdraft_limit,
        cast(interest_rate      as decimal(8,4))                    as interest_rate_pct,
        cast(monthly_fee        as decimal(10,2))                   as monthly_fee_zar,

        -- Banking details
        trim(account_number)                                        as account_number,
        trim(iban)                                                   as iban,
        trim(swift_bic)                                             as swift_bic,

        -- Audit
        cast(created_at         as timestamp)                       as created_at,
        cast(updated_at         as timestamp)                       as updated_at,
        cast(_ingested_at       as timestamp)                       as _ingested_at

    from source
    where account_id is not null
      and customer_id is not null
),

enriched as (
    select
        *,

        -- ── Account age ──────────────────────────────────────────────────────
        datediff(
            coalesce(account_close_date, current_date()),
            account_open_date
        )                                                           as account_age_days,

        -- Days since last transaction
        datediff(current_date(), last_transaction_date)             as days_since_last_txn,

        -- ── Dormancy flag (SARB Dormant Accounts Act threshold: 3 months) ───
        case
            when last_transaction_date is null                          then true
            when datediff(current_date(), last_transaction_date) > {{ var('dormant_days') }}
                                                                        then true
            else false
        end                                                         as is_dormant,

        -- ── Balance band (ZAR) ───────────────────────────────────────────────
        case
            when current_balance < 0           then 'Negative'
            when current_balance < 1000        then '0–1K'
            when current_balance < 10000       then '1K–10K'
            when current_balance < 100000      then '10K–100K'
            when current_balance < 1000000     then '100K–1M'
            else                                    '1M+'
        end                                                         as balance_band,

        -- ── Overdraft utilisation ────────────────────────────────────────────
        case
            when overdraft_limit > 0
            then round(
                    greatest(0, (overdraft_limit - available_balance)) / overdraft_limit * 100,
                 2)
            else 0
        end                                                         as overdraft_utilisation_pct,

        -- ── Account status normalisation ─────────────────────────────────────
        -- Reconcile source status with derived dormancy
        case
            when account_status = 'CLOSED'                          then 'CLOSED'
            when account_status = 'FROZEN'                          then 'FROZEN'
            when datediff(current_date(), last_transaction_date) > {{ var('dormant_days') }}
                                                                    then 'DORMANT'
            else 'ACTIVE'
        end                                                         as account_status_derived,

        -- ── Interest-bearing flag ────────────────────────────────────────────
        case when interest_rate_pct > 0 then true else false end    as is_interest_bearing,

        -- ── Fixed deposit matured flag ────────────────────────────────────────
        case
            when account_type = 'FIXED_DEPOSIT'
             and maturity_date < current_date()                     then true
            else false
        end                                                         as is_matured,

        current_timestamp()                                         as loaded_at

    from cast_and_rename
),

final as (
    select
        account_id,
        customer_id,
        branch_id,
        account_number,
        iban,
        swift_bic,
        account_type,
        account_status,
        account_status_derived,
        product_code,
        currency_code,
        account_open_date,
        account_close_date,
        account_age_days,
        maturity_date,
        last_transaction_date,
        days_since_last_txn,
        is_dormant,
        current_balance,
        available_balance,
        overdraft_limit,
        overdraft_utilisation_pct,
        balance_band,
        interest_rate_pct,
        monthly_fee_zar,
        is_interest_bearing,
        is_matured,
        created_at,
        updated_at,
        _ingested_at,
        loaded_at
    from enriched
)

select * from final
