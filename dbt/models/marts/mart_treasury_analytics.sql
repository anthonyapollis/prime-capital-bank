{{
    config(
        materialized = 'table',
        tags         = ['mart', 'treasury', 'fx', 'var', 'daily'],
        description  = 'Treasury analytics: P&L, position sizing, FX exposure, duration, yield estimates, and VaR proxies.'
    )
}}

/*
  mart_treasury_analytics
  ─────────────────────────────────────────────────────────────────────────────
  Sources : stg_accounts, stg_transactions, stg_payments, int_payment_analytics,
            stg_loans
  Layer   : Gold (mart)
  Grain   : One row per currency × account_type combination (position-level)
  Purpose : Treasury management analytics covering balance sheet positions,
            net interest income, FX exposure by currency, duration risk,
            and parametric VaR estimates (simplified 99% confidence).
  ─────────────────────────────────────────────────────────────────────────────
*/

with

-- ── Asset positions: loans ────────────────────────────────────────────────────
loan_positions as (
    select
        'ZAR'                                                       as currency_code,
        loan_type                                                   as instrument_type,
        'LOAN_ASSET'                                                as position_type,
        count(loan_id)                                              as instrument_count,
        sum(outstanding_principal_zar)                              as position_balance_zar,
        avg(interest_rate_pct)                                      as avg_yield_pct,
        -- Duration approximation: weighted average remaining term / 12
        avg(remaining_term_months / 12.0)                           as avg_duration_years,
        sum(monthly_instalment_zar * remaining_term_months)         as total_cash_flows_zar
    from {{ ref('stg_loans') }}
    where loan_status = 'ACTIVE'
    group by loan_type
),

-- ── Liability positions: deposits by account type ─────────────────────────────
deposit_positions as (
    select
        currency_code,
        account_type                                                as instrument_type,
        'DEPOSIT_LIABILITY'                                         as position_type,
        count(account_id)                                           as instrument_count,
        sum(current_balance)                                        as position_balance_zar,
        avg(interest_rate_pct)                                      as avg_yield_pct,
        -- Duration: current/savings = ~0 (on-demand); FD = term months
        avg(case
            when account_type = 'FIXED_DEPOSIT'
            then datediff(coalesce(maturity_date, current_date()), current_date()) / 365.0
            else 0.0
        end)                                                        as avg_duration_years,
        0                                                           as total_cash_flows_zar
    from {{ ref('stg_accounts') }}
    where account_status_derived = 'ACTIVE'
    group by currency_code, account_type
),

-- ── FX exposure: payments and accounts in non-ZAR currencies ─────────────────
fx_positions as (
    select
        currency_code,
        'FX_POSITION'                                               as instrument_type,
        'FX_EXPOSURE'                                               as position_type,
        count(account_id)                                           as instrument_count,
        sum(current_balance)                                        as position_balance_zar,
        0                                                           as avg_yield_pct,
        0                                                           as avg_duration_years,
        0                                                           as total_cash_flows_zar
    from {{ ref('stg_accounts') }}
    where currency_code != 'ZAR'
      and account_status_derived = 'ACTIVE'
    group by currency_code
),

-- ── SWIFT payment FX flow ─────────────────────────────────────────────────────
swift_flows as (
    select
        date_trunc('month', payment_date)                           as flow_month,
        payment_rail,
        beneficiary_country_code                                    as destination_country,
        count(payment_id)                                           as payment_count,
        sum(payment_amount_zar)                                     as flow_value_zar,
        avg(settlement_lag_days)                                    as avg_settlement_days
    from {{ ref('stg_payments') }}
    where payment_rail = 'SWIFT'
      and payment_status = 'SETTLED'
      and payment_date >= date_add(current_date(), -365)
    group by date_trunc('month', payment_date), payment_rail, beneficiary_country_code
),

-- ── Combine asset and liability positions ─────────────────────────────────────
all_positions as (
    select * from loan_positions
    union all
    select * from deposit_positions
    union all
    select * from fx_positions
),

-- ── Net Interest Income (NII) estimate ────────────────────────────────────────
-- NII = Interest income from assets − Interest expense on liabilities
-- Using average ZAR balance × average rate
nii_estimate as (
    select
        instrument_type,
        position_type,
        position_balance_zar,
        avg_yield_pct,
        avg_duration_years,
        -- Annual interest income / expense
        round(position_balance_zar * avg_yield_pct / 100.0, 2)     as annual_interest_zar,
        -- Net Interest Margin contribution (income positive, expense negative)
        round(
            position_balance_zar * avg_yield_pct / 100.0 *
            case when position_type = 'DEPOSIT_LIABILITY' then -1 else 1 end,
        2)                                                          as nim_contribution_zar
    from all_positions
),

-- ── Duration and interest rate sensitivity ────────────────────────────────────
-- DV01 = dollar value of a 1bp (0.01%) move in rates
-- DV01 ≈ position_balance × duration × 0.0001
duration_risk as (
    select
        *,
        round(position_balance_zar * avg_duration_years * 0.0001, 0) as dv01_zar,
        -- Modified duration sensitivity (parallel shift ±100bp)
        round(position_balance_zar * avg_duration_years * 0.01, 0)   as ir_sensitivity_100bp_zar
    from nii_estimate
),

-- ── Parametric VaR (simplified) ───────────────────────────────────────────────
-- 1-day 99% VaR = Position × Volatility × 2.326 (z-score for 99%)
-- Using typical daily volatility assumptions by asset class
var_estimates as (
    select
        *,
        -- Daily volatility assumption by instrument
        case
            when position_type = 'LOAN_ASSET'       then 0.0005   -- 5bps daily vol
            when position_type = 'FX_EXPOSURE'      then 0.008    -- 80bps FX daily vol
            when position_type = 'DEPOSIT_LIABILITY' then 0.0002  -- stable funding
            else 0.001
        end                                                         as daily_vol_assumption,

        round(
            position_balance_zar *
            case
                when position_type = 'LOAN_ASSET'       then 0.0005
                when position_type = 'FX_EXPOSURE'      then 0.008
                when position_type = 'DEPOSIT_LIABILITY' then 0.0002
                else 0.001
            end * 2.326,    -- 99% z-score
        0)                                                          as var_1day_99pct_zar,

        -- 10-day VaR (Basel market risk requirement) = 1-day × sqrt(10)
        round(
            position_balance_zar *
            case
                when position_type = 'LOAN_ASSET'       then 0.0005
                when position_type = 'FX_EXPOSURE'      then 0.008
                when position_type = 'DEPOSIT_LIABILITY' then 0.0002
                else 0.001
            end * 2.326 * 3.162,    -- sqrt(10)
        0)                                                          as var_10day_99pct_zar

    from duration_risk
),

-- ── Treasury P&L summary ──────────────────────────────────────────────────────
treasury_pl as (
    select
        sum(nim_contribution_zar)                                   as total_nim_zar,
        sum(case when position_type = 'LOAN_ASSET'    then position_balance_zar else 0 end)
                                                                    as total_asset_book_zar,
        sum(case when position_type = 'DEPOSIT_LIABILITY' then position_balance_zar else 0 end)
                                                                    as total_liability_book_zar,
        sum(case when position_type = 'FX_EXPOSURE'   then position_balance_zar else 0 end)
                                                                    as total_fx_exposure_zar,
        sum(var_1day_99pct_zar)                                     as total_var_1day_zar,
        sum(var_10day_99pct_zar)                                     as total_var_10day_zar,
        sum(dv01_zar)                                               as total_dv01_zar,
        sum(ir_sensitivity_100bp_zar)                               as total_ir_sensitivity_100bp_zar
    from var_estimates
),

final as (
    select
        v.instrument_type,
        v.position_type,
        v.instrument_count,
        v.position_balance_zar,
        v.avg_yield_pct,
        v.avg_duration_years,
        v.annual_interest_zar,
        v.nim_contribution_zar,
        v.dv01_zar,
        v.ir_sensitivity_100bp_zar,
        v.daily_vol_assumption,
        v.var_1day_99pct_zar,
        v.var_10day_99pct_zar,

        -- Portfolio-level totals (attached to every row for dashboard)
        p.total_nim_zar,
        p.total_asset_book_zar,
        p.total_liability_book_zar,
        p.total_fx_exposure_zar,
        p.total_var_1day_zar,
        p.total_var_10day_zar,
        p.total_dv01_zar,
        p.total_ir_sensitivity_100bp_zar,

        -- Net Balance Sheet gap
        round(p.total_asset_book_zar - p.total_liability_book_zar, 0) as balance_sheet_gap_zar,

        current_date()                                              as reporting_date,
        current_timestamp()                                         as loaded_at

    from var_estimates v
    cross join treasury_pl p
)

select * from final
