{{
    config(
        materialized = 'table',
        tags         = ['mart', 'wealth', 'asset_management', 'regulatory', 'daily'],
        description  = 'Portfolio-level wealth mart: AUM, asset allocation look-through, unrealised P&L, concentration and Regulation 28 compliance status.'
    )
}}

/*
  mart_portfolio_analytics
  ─────────────────────────────────────────────────────────────────────────────
  Sources : stg_portfolios, stg_portfolio_holdings
  Layer   : Gold (mart)
  Grain   : One row per portfolio (portfolio_id)
  Purpose : The wealth division's reporting backbone. Pivots each portfolio's
            positions into Regulation 28 buckets, tests them against the
            statutory limits, and surfaces single-issuer concentration.

            Regulation 28 of the Pension Funds Act caps a retirement vehicle's
            exposure: equity 75%, offshore 45%, property 25%. Breaches are
            usually drift rather than intent — a rally moves the weights after
            the last rebalance — but they are reportable either way, so the
            model states the excess in rand as well as percentage points.
  ─────────────────────────────────────────────────────────────────────────────
*/

{% set equity_limit   = 0.75 %}
{% set offshore_limit = 0.45 %}
{% set property_limit = 0.25 %}

with

portfolios as (
    select * from {{ ref('stg_portfolios') }}
),

holdings as (
    select * from {{ ref('stg_portfolio_holdings') }}
),

-- one row per portfolio, allocation pivoted into the Reg 28 buckets
allocation as (
    select
        portfolio_id,
        count(*)                                                    as position_count,
        count(distinct ticker)                                      as instrument_count,
        count(distinct sector)                                      as sector_count,
        sum(market_value_zar)                                       as holdings_value_zar,
        sum(cost_basis_zar)                                         as cost_basis_zar,
        sum(unrealised_pnl_zar)                                     as unrealised_pnl_zar,
        max(market_value_zar)                                       as largest_position_zar,

        sum(case when reg28_bucket = 'EQUITY'   then market_value_zar else 0 end) as equity_zar,
        sum(case when reg28_bucket = 'OFFSHORE' then market_value_zar else 0 end) as offshore_zar,
        sum(case when reg28_bucket = 'PROPERTY' then market_value_zar else 0 end) as property_zar,
        sum(case when reg28_bucket = 'BOND'     then market_value_zar else 0 end) as bond_zar,
        sum(case when reg28_bucket = 'CASH'     then market_value_zar else 0 end) as cash_zar
    from holdings
    group by portfolio_id
),

combined as (
    select
        p.portfolio_id,
        p.customer_id,
        p.adviser_id,
        p.mandate,
        p.risk_profile,
        p.account_type,
        p.benchmark,
        p.province,
        p.currency_code,
        p.inception_date,
        p.portfolio_age_years,
        p.is_short_track_record,
        p.fee_bps,
        p.annual_fee_zar,
        p.aum_band,
        p.is_reg28_regulated,
        p.market_value_zar,

        coalesce(a.position_count, 0)                               as position_count,
        coalesce(a.instrument_count, 0)                             as instrument_count,
        coalesce(a.sector_count, 0)                                 as sector_count,
        coalesce(a.holdings_value_zar, 0)                           as holdings_value_zar,
        coalesce(a.cost_basis_zar, 0)                               as cost_basis_zar,
        coalesce(a.unrealised_pnl_zar, 0)                           as unrealised_pnl_zar,
        coalesce(a.largest_position_zar, 0)                         as largest_position_zar,

        coalesce(a.equity_zar, 0)                                   as equity_zar,
        coalesce(a.offshore_zar, 0)                                 as offshore_zar,
        coalesce(a.property_zar, 0)                                 as property_zar,
        coalesce(a.bond_zar, 0)                                     as bond_zar,
        coalesce(a.cash_zar, 0)                                     as cash_zar
    from portfolios p
    left join allocation a
        on p.portfolio_id = a.portfolio_id
),

weighted as (
    select
        *,
        -- guard the divisor: a funded-but-uninvested mandate has no holdings yet
        case when holdings_value_zar > 0 then equity_zar   / holdings_value_zar end as equity_pct,
        case when holdings_value_zar > 0 then offshore_zar / holdings_value_zar end as offshore_pct,
        case when holdings_value_zar > 0 then property_zar / holdings_value_zar end as property_pct,
        case when holdings_value_zar > 0 then bond_zar     / holdings_value_zar end as bond_pct,
        case when holdings_value_zar > 0 then cash_zar     / holdings_value_zar end as cash_pct,

        -- single-issuer concentration: the second-order risk after Reg 28
        case when holdings_value_zar > 0
             then largest_position_zar / holdings_value_zar end                     as largest_position_pct,

        case when cost_basis_zar > 0
             then round((holdings_value_zar / cost_basis_zar - 1) * 100, 2) end     as unrealised_pnl_pct
    from combined
),

compliance as (
    select
        *,
        -- statutory tests, only meaningful where Reg 28 actually applies
        case when is_reg28_regulated and equity_pct   > {{ equity_limit }}   then true else false end as breach_equity,
        case when is_reg28_regulated and offshore_pct > {{ offshore_limit }} then true else false end as breach_offshore,
        case when is_reg28_regulated and property_pct > {{ property_limit }} then true else false end as breach_property,

        greatest(round((coalesce(equity_pct,0)   - {{ equity_limit }})   * holdings_value_zar, 2), 0) as equity_excess_zar,
        greatest(round((coalesce(offshore_pct,0) - {{ offshore_limit }}) * holdings_value_zar, 2), 0) as offshore_excess_zar,
        greatest(round((coalesce(property_pct,0) - {{ property_limit }}) * holdings_value_zar, 2), 0) as property_excess_zar
    from weighted
),

final as (
    select
        *,
        (case when breach_equity   then 1 else 0 end +
         case when breach_offshore then 1 else 0 end +
         case when breach_property then 1 else 0 end)               as reg28_breach_count,

        case when is_reg28_regulated then
            case when breach_equity or breach_offshore or breach_property
                 then 'BREACH' else 'COMPLIANT' end
        else 'NOT_APPLICABLE' end                                   as reg28_status,

        case when is_reg28_regulated
             then equity_excess_zar + offshore_excess_zar + property_excess_zar
             else 0 end                                             as total_excess_zar,

        -- plain-English action for the adviser, so the report explains itself
        case
            when is_reg28_regulated and (breach_equity or breach_offshore or breach_property)
                then 'Rebalance to statutory limits'
            when largest_position_pct > 0.25
                then 'Review single-issuer concentration'
            when instrument_count <= 3
                then 'Diversify: fewer than four instruments'
            when is_short_track_record
                then 'Monitor: track record under two years'
            else 'No action'
        end                                                         as recommended_action
    from compliance
)

select * from final
