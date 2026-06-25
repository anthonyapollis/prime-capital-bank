{{
    config(
        materialized = 'view',
        tags         = ['staging', 'transactions', 'daily'],
        description  = 'Staged transaction records: type standardisation, ZAR conversion, credit/debit flags, channel codes.'
    )
}}

/*
  stg_transactions
  ─────────────────────────────────────────────────────────────────────────────
  Source  : prime_capital.bronze.bronze_transactions
  Layer   : Silver (staging)
  Grain   : One row per transaction (transaction_id)
  Purpose : Standardise transaction types to a controlled enum, convert all
            currency amounts to ZAR using the exchange_rate macro, and tag
            each transaction with is_credit / is_debit and a channel_code.
  ─────────────────────────────────────────────────────────────────────────────
*/

with

source as (
    select * from {{ source('bronze', 'bronze_transactions') }}
),

-- Exchange rates seed/lookup table (populated daily from treasury feed)
fx_rates as (
    select * from {{ source('bronze', 'bronze_fx_rates') }}
    where rate_date = current_date()
),

cast_and_rename as (
    select
        -- Keys
        cast(transaction_id         as string)                      as transaction_id,
        cast(account_id             as string)                      as account_id,
        cast(customer_id            as string)                      as customer_id,
        cast(branch_id              as string)                      as branch_id,
        cast(teller_id              as string)                      as teller_id,

        -- Dates & times
        cast(transaction_date       as date)                        as transaction_date,
        cast(transaction_timestamp  as timestamp)                   as transaction_timestamp,
        cast(value_date             as date)                        as value_date,
        cast(posting_date           as date)                        as posting_date,

        -- Amounts
        cast(transaction_amount     as decimal(18,2))               as transaction_amount_orig,
        upper(trim(currency_code))                                  as currency_code,

        -- Type & channel (raw — standardised below)
        upper(trim(transaction_type))                               as transaction_type_raw,
        upper(trim(channel))                                        as channel_raw,
        upper(trim(transaction_status))                             as transaction_status,
        -- COMPLETED / PENDING / REVERSED / FAILED

        -- Narration & reference
        trim(narration)                                             as narration,
        trim(reference_number)                                      as reference_number,
        trim(counterparty_account)                                  as counterparty_account,
        trim(counterparty_name)                                     as counterparty_name,
        upper(trim(counterparty_bank_code))                         as counterparty_bank_code,

        -- Fees charged on the transaction
        cast(transaction_fee        as decimal(10,2))               as transaction_fee_orig,

        -- Audit
        cast(created_at             as timestamp)                   as created_at,
        cast(_ingested_at           as timestamp)                   as _ingested_at

    from source
    where transaction_id is not null
      and account_id     is not null
),

-- ── Standardise transaction type to controlled enum ────────────────────────────
type_standardised as (
    select
        *,
        case
            when transaction_type_raw in ('CREDIT', 'CR', 'DEPOSIT', 'DEP')        then 'CREDIT'
            when transaction_type_raw in ('DEBIT',  'DR', 'WITHDRAWAL', 'WD')      then 'DEBIT'
            when transaction_type_raw in ('TRANSFER', 'TRF', 'INT_TRANSFER')       then 'TRANSFER'
            when transaction_type_raw in ('PAYMENT', 'PAY', 'EFT_PAYMENT')         then 'PAYMENT'
            when transaction_type_raw in ('INTEREST', 'INT', 'INTEREST_CREDIT')    then 'INTEREST'
            when transaction_type_raw in ('FEE', 'BANK_FEE', 'SERVICE_CHARGE')     then 'FEE'
            when transaction_type_raw in ('REVERSAL', 'REV', 'RECALL')             then 'REVERSAL'
            when transaction_type_raw in ('FOREX', 'FX', 'CURRENCY_CONVERSION')    then 'FOREX'
            else 'OTHER'
        end                                                         as transaction_type,

        -- is_credit: money entering the account
        case
            when transaction_type_raw in ('CREDIT','CR','DEPOSIT','DEP','INTEREST',
                                          'INT','INTEREST_CREDIT')                  then true
            else false
        end                                                         as is_credit,

        -- is_debit: money leaving the account
        case
            when transaction_type_raw in ('DEBIT','DR','WITHDRAWAL','WD','PAYMENT',
                                          'PAY','EFT_PAYMENT','FEE','BANK_FEE',
                                          'SERVICE_CHARGE')                         then true
            else false
        end                                                         as is_debit

    from cast_and_rename
),

-- ── Standardise channel to enum ───────────────────────────────────────────────
channel_standardised as (
    select
        *,
        case
            when channel_raw in ('BRANCH', 'TELLER', 'COUNTER')        then 'BRANCH'
            when channel_raw in ('ATM', 'CASH_MACHINE')                 then 'ATM'
            when channel_raw in ('INTERNET', 'ONLINE', 'WEB', 'IB')    then 'INTERNET_BANKING'
            when channel_raw in ('MOBILE', 'APP', 'MBANKING', 'USSD')  then 'MOBILE_BANKING'
            when channel_raw in ('POS', 'POINT_OF_SALE', 'CARD_POS')   then 'POS'
            when channel_raw in ('EFT', 'EXTERNAL_EFT')                 then 'EFT'
            when channel_raw in ('RTGS', 'SWIFT', 'WIRE')              then 'WIRE'
            when channel_raw in ('API', 'OPEN_BANKING')                 then 'API'
            else 'OTHER'
        end                                                         as channel_code

    from type_standardised
),

-- ── FX conversion to ZAR ──────────────────────────────────────────────────────
fx_converted as (
    select
        t.*,
        coalesce(fx.zar_rate, 1.0)                                  as zar_rate,
        -- Convert transaction amount to ZAR
        {{ convert_to_zar('t.transaction_amount_orig', 't.currency_code') }}
                                                                    as transaction_amount_zar,
        {{ convert_to_zar('t.transaction_fee_orig', 't.currency_code') }}
                                                                    as transaction_fee_zar

    from channel_standardised t
    left join fx_rates fx
        on t.currency_code = fx.from_currency
),

final as (
    select
        transaction_id,
        account_id,
        customer_id,
        branch_id,
        teller_id,
        transaction_date,
        transaction_timestamp,
        value_date,
        posting_date,
        transaction_type,
        transaction_type_raw,
        is_credit,
        is_debit,
        transaction_amount_orig,
        currency_code,
        zar_rate,
        transaction_amount_zar,
        transaction_fee_orig,
        transaction_fee_zar,
        channel_code,
        channel_raw,
        transaction_status,
        narration,
        reference_number,
        counterparty_account,
        counterparty_name,
        counterparty_bank_code,
        created_at,
        _ingested_at,
        current_timestamp()                                         as loaded_at
    from fx_converted
)

select * from final
