{{
    config(
        materialized = 'view',
        tags         = ['staging', 'credit_cards', 'daily'],
        description  = 'Staged credit card records: utilisation, minimum payment flag, overlimit, and reward tier.'
    )
}}

/*
  stg_credit_cards
  ─────────────────────────────────────────────────────────────────────────────
  Source  : prime_capital.bronze.bronze_credit_cards
  Layer   : Silver (staging)
  Grain   : One row per credit card account (card_id)
  Purpose : Compute utilisation_rate, flag minimum payment behaviour and
            overlimit accounts, and assign reward tier based on spend profile.
  ─────────────────────────────────────────────────────────────────────────────
*/

with

source as (
    select * from {{ source('bronze', 'bronze_credit_cards') }}
),

cast_and_rename as (
    select
        -- Keys
        cast(card_id                as string)                      as card_id,
        cast(account_id             as string)                      as account_id,
        cast(customer_id            as string)                      as customer_id,

        -- Card descriptors
        upper(trim(card_type))                                      as card_type,
        -- VISA / MASTERCARD / AMEX / DINERS
        upper(trim(card_tier))                                      as card_tier,
        -- ENTRY / STANDARD / GOLD / PLATINUM / BLACK / INFINITE
        upper(trim(card_status))                                    as card_status,
        -- ACTIVE / BLOCKED / EXPIRED / CANCELLED / HOT_LISTED
        upper(trim(currency_code))                                  as currency_code,

        -- Dates
        cast(issue_date             as date)                        as issue_date,
        cast(expiry_date            as date)                        as expiry_date,
        cast(last_statement_date    as date)                        as last_statement_date,
        cast(next_payment_due_date  as date)                        as next_payment_due_date,
        cast(last_payment_date      as date)                        as last_payment_date,

        -- Financials
        cast(credit_limit           as decimal(14,2))               as credit_limit_zar,
        cast(current_balance        as decimal(14,2))               as current_balance_zar,
        cast(available_credit       as decimal(14,2))               as available_credit_zar,
        cast(minimum_payment_due    as decimal(12,2))               as minimum_payment_due_zar,
        cast(last_payment_amount    as decimal(12,2))               as last_payment_amount_zar,
        cast(statement_balance      as decimal(14,2))               as statement_balance_zar,
        cast(cash_advance_limit     as decimal(12,2))               as cash_advance_limit_zar,
        cast(cash_advance_balance   as decimal(12,2))               as cash_advance_balance_zar,

        -- Spend (current statement period)
        cast(total_purchases_mtd    as decimal(14,2))               as total_purchases_mtd_zar,
        cast(total_cash_advances_mtd as decimal(12,2))              as total_cash_advances_mtd_zar,
        cast(rewards_points_balance as decimal(14,2))               as rewards_points_balance,
        cast(annual_fee             as decimal(10,2))               as annual_fee_zar,

        -- Interest
        cast(purchase_interest_rate as decimal(8,4))                as purchase_interest_rate_pct,
        cast(cash_advance_rate      as decimal(8,4))                as cash_advance_rate_pct,

        -- Audit
        cast(created_at             as timestamp)                   as created_at,
        cast(updated_at             as timestamp)                   as updated_at,
        cast(_ingested_at           as timestamp)                   as _ingested_at

    from source
    where card_id is not null
),

utilisation as (
    select
        *,

        -- ── Credit utilisation rate ───────────────────────────────────────────
        -- Regulators flag >75% as high utilisation; scoring models use this heavily
        case
            when credit_limit_zar > 0
            then round(current_balance_zar / credit_limit_zar * 100, 2)
            else null
        end                                                         as utilisation_rate_pct,

        -- ── Utilisation band ─────────────────────────────────────────────────
        case
            when credit_limit_zar <= 0                              then 'No Limit'
            when current_balance_zar / credit_limit_zar <= 0.30    then 'Low (0–30%)'
            when current_balance_zar / credit_limit_zar <= 0.60    then 'Moderate (30–60%)'
            when current_balance_zar / credit_limit_zar <= 0.90    then 'High (60–90%)'
            else                                                         'Very High (>90%)'
        end                                                         as utilisation_band,

        -- ── Overlimit flag ───────────────────────────────────────────────────
        case
            when current_balance_zar > credit_limit_zar then true
            else false
        end                                                         as is_overlimit,

        -- ── Overlimit excess amount ───────────────────────────────────────────
        case
            when current_balance_zar > credit_limit_zar
            then round(current_balance_zar - credit_limit_zar, 2)
            else 0.00
        end                                                         as overlimit_amount_zar,

        -- ── Minimum payment flag (did customer pay only the minimum?) ─────────
        case
            when last_payment_amount_zar > 0
             and last_payment_amount_zar <= minimum_payment_due_zar * 1.05  -- 5% tolerance
             and last_payment_amount_zar < statement_balance_zar            -- not full pay
            then true
            else false
        end                                                         as minimum_payment_flag,

        -- ── Full payment flag (transactor behaviour — no revolving balance) ────
        case
            when last_payment_amount_zar >= statement_balance_zar then true
            else false
        end                                                         as full_payment_flag,

        -- ── Missed payment flag ───────────────────────────────────────────────
        case
            when next_payment_due_date < current_date()
             and coalesce(last_payment_date, '1900-01-01') < last_statement_date
            then true
            else false
        end                                                         as missed_payment_flag,

        -- ── Cash advance heavy user flag (>20% of limit used for cash) ────────
        case
            when cash_advance_limit_zar > 0
             and cash_advance_balance_zar / cash_advance_limit_zar > 0.20  then true
            else false
        end                                                         as heavy_cash_advance_flag,

        -- ── Card expiry status ────────────────────────────────────────────────
        case
            when expiry_date < current_date()                       then 'Expired'
            when datediff(expiry_date, current_date()) <= 90        then 'Expiring Soon'
            else 'Valid'
        end                                                         as expiry_status

    from cast_and_rename
),

reward_tier_assigned as (
    select
        *,

        -- ── Reward tier (based on annual spend and card tier) ─────────────────
        -- PCB Rewards programme: higher spend = better earn rate and perks
        case
            when total_purchases_mtd_zar * 12 >= 500000 or card_tier = 'BLACK'    then 'Platinum'
            when total_purchases_mtd_zar * 12 >= 200000 or card_tier = 'PLATINUM' then 'Gold'
            when total_purchases_mtd_zar * 12 >= 60000  or card_tier = 'GOLD'     then 'Silver'
            when total_purchases_mtd_zar * 12 >= 12000  or card_tier = 'STANDARD' then 'Bronze'
            else                                                                        'Standard'
        end                                                         as reward_tier

    from utilisation
),

final as (
    select
        card_id,
        account_id,
        customer_id,
        card_type,
        card_tier,
        card_status,
        expiry_status,
        currency_code,
        issue_date,
        expiry_date,
        last_statement_date,
        next_payment_due_date,
        last_payment_date,
        credit_limit_zar,
        current_balance_zar,
        available_credit_zar,
        utilisation_rate_pct,
        utilisation_band,
        is_overlimit,
        overlimit_amount_zar,
        minimum_payment_due_zar,
        last_payment_amount_zar,
        statement_balance_zar,
        minimum_payment_flag,
        full_payment_flag,
        missed_payment_flag,
        heavy_cash_advance_flag,
        cash_advance_limit_zar,
        cash_advance_balance_zar,
        total_purchases_mtd_zar,
        total_cash_advances_mtd_zar,
        rewards_points_balance,
        reward_tier,
        annual_fee_zar,
        purchase_interest_rate_pct,
        cash_advance_rate_pct,
        created_at,
        updated_at,
        _ingested_at,
        current_timestamp()                                         as loaded_at
    from reward_tier_assigned
)

select * from final
