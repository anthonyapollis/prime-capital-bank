{{
    config(
        materialized = 'view',
        tags         = ['staging', 'wealth', 'asset_management', 'daily'],
        description  = 'Staged discretionary portfolio master: mandate, risk profile, benchmark, adviser and Reg 28 applicability.'
    )
}}

/*
  stg_portfolios
  ─────────────────────────────────────────────────────────────────────────────
  Source  : prime_capital.bronze.bronze_portfolios
  Layer   : Silver (staging)
  Grain   : One row per portfolio (portfolio_id)
  Purpose : Clean the wealth-management mandate book. Derives portfolio tenure,
            AUM band, and the annual management fee implied by the fee basis
            points on the mandate. Flags whether the portfolio is subject to
            Regulation 28 of the Pension Funds Act — retirement vehicles always
            are, regardless of the mandate's stated house limits.
  ─────────────────────────────────────────────────────────────────────────────
*/

with

source as (
    select * from {{ source('bronze', 'bronze_portfolios') }}
),

cast_and_rename as (
    select
        -- Keys
        cast(portfolio_id    as string)                             as portfolio_id,
        cast(customer_id     as string)                             as customer_id,
        cast(adviser_id      as string)                             as adviser_id,

        -- Mandate descriptors
        trim(mandate)                                               as mandate,
        upper(trim(risk_profile))                                   as risk_profile,
        -- CONSERVATIVE / MODERATE / AGGRESSIVE
        trim(account_type)                                          as account_type,
        -- Retirement Annuity / Pension Fund / Living Annuity / Discretionary /
        -- Tax-Free Savings / Endowment
        trim(benchmark)                                             as benchmark,
        -- ALSI / SWIX / ALBI / MSCI World / STeFI / STeFI + 4% / SAPY
        upper(trim(currency))                                       as currency_code,
        trim(province)                                              as province,

        -- Dates & economics
        cast(inception_date  as date)                               as inception_date,
        cast(fee_bps         as int)                                as fee_bps,
        cast(market_value_zar as decimal(18,2))                     as market_value_zar,

        -- Regulation 28 applicability
        case
            when cast(reg28_regulated as int) = 1                          then true
            when trim(account_type) in ('Retirement Annuity','Pension Fund') then true
            else false
        end                                                         as is_reg28_regulated
    from source
),

enriched as (
    select
        *,
        datediff(current_date(), inception_date)                    as portfolio_age_days,
        round(datediff(current_date(), inception_date) / 365.25, 1) as portfolio_age_years,

        -- annual management fee at the mandate's fee basis points
        round(market_value_zar * fee_bps / 10000.0, 2)              as annual_fee_zar,

        case
            when market_value_zar >= 100000000 then 'INSTITUTIONAL'   -- R100m+
            when market_value_zar >=  25000000 then 'ULTRA_HIGH'      -- R25m+
            when market_value_zar >=   5000000 then 'HIGH_NET_WORTH'  -- R5m+
            when market_value_zar >=   1000000 then 'AFFLUENT'        -- R1m+
            else 'RETAIL'
        end                                                         as aum_band,

        -- a mandate under two years has not yet seen a full market cycle;
        -- performance attribution should be treated with caution
        case when datediff(current_date(), inception_date) < 730
             then true else false end                               as is_short_track_record
    from cast_and_rename
)

select * from enriched
