{{
    config(
        materialized = 'view',
        tags         = ['staging', 'wealth', 'asset_management', 'daily'],
        description  = 'Staged portfolio positions joined to the security master: market value, unrealised P&L and asset-class look-through.'
    )
}}

/*
  stg_portfolio_holdings
  ─────────────────────────────────────────────────────────────────────────────
  Sources : prime_capital.bronze.bronze_portfolio_holdings
            prime_capital.bronze.bronze_securities
  Layer   : Silver (staging)
  Grain   : One row per portfolio per instrument (portfolio_id, ticker)
  Purpose : Position-level view with the security master joined on, so every
            downstream model can aggregate by asset class, sector or listing
            without re-joining. Prices are held in cents (JSE convention) and
            converted to rand here, once, so no downstream model repeats it.
  ─────────────────────────────────────────────────────────────────────────────
*/

with

holdings as (
    select * from {{ source('bronze', 'bronze_portfolio_holdings') }}
),

securities as (
    select
        cast(ticker as string)                                      as ticker,
        trim(security_name)                                         as security_name,
        trim(asset_class)                                           as asset_class,
        trim(sector)                                                as sector,
        trim(listing)                                               as listing
    from {{ source('bronze', 'bronze_securities') }}
),

joined as (
    select
        cast(h.portfolio_id as string)                              as portfolio_id,
        cast(h.ticker       as string)                              as ticker,
        s.security_name,
        s.asset_class,
        s.sector,
        s.listing,

        cast(h.units             as decimal(18,4))                  as units,
        -- JSE quotes in cents; hold rand at position level
        cast(h.avg_cost_cents    as decimal(18,2)) / 100.0          as avg_cost_zar,
        cast(h.last_price_cents  as decimal(18,2)) / 100.0          as last_price_zar,
        cast(h.market_value_zar  as decimal(18,2))                  as market_value_zar,
        cast(h.valuation_date    as date)                           as valuation_date
    from holdings h
    left join securities s
        on cast(h.ticker as string) = s.ticker
),

enriched as (
    select
        *,
        round(units * avg_cost_zar, 2)                              as cost_basis_zar,
        round(market_value_zar - (units * avg_cost_zar), 2)         as unrealised_pnl_zar,
        case
            when units * avg_cost_zar > 0
            then round((market_value_zar / (units * avg_cost_zar) - 1) * 100, 2)
        end                                                         as unrealised_pnl_pct,

        -- Reg 28 buckets: offshore and property are tested separately from
        -- local equity, so classify once here rather than in every mart
        case
            when asset_class in ('Equity', 'ETF') then 'EQUITY'
            when asset_class = 'Offshore'          then 'OFFSHORE'
            when asset_class = 'Property'          then 'PROPERTY'
            when asset_class = 'Bond'              then 'BOND'
            when asset_class = 'Cash'              then 'CASH'
            else 'OTHER'
        end                                                         as reg28_bucket
    from joined
)

select * from enriched
