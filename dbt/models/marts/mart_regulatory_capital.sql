{{
    config(
        materialized = 'table',
        tags         = ['mart', 'regulatory', 'basel3', 'capital', 'daily'],
        description  = 'Basel III regulatory capital mart: RWA by exposure class, CET1, Tier 1, leverage ratio, LCR, NSFR.'
    )
}}

/*
  mart_regulatory_capital
  ─────────────────────────────────────────────────────────────────────────────
  Sources : stg_loans, stg_accounts, stg_credit_cards, int_loan_health,
            stg_transactions
  Layer   : Gold (mart)
  Grain   : One row per exposure class (Basel III standardised approach)
  Purpose : SARB BA 700 / Basel III capital adequacy reporting. Computes:
              • Risk-Weighted Assets (RWA) by exposure class
              • CET1 capital ratio, Tier 1 ratio, Total Capital ratio
              • Leverage ratio (Tier 1 / Total Exposure)
              • Liquidity Coverage Ratio (LCR) — simplified HQLA / NCOF
              • Net Stable Funding Ratio (NSFR) — simplified ASF / RSF
  Note    : In production, capital positions flow from the General Ledger.
            This model uses loan/deposit balances as proxies for demonstration.
  ─────────────────────────────────────────────────────────────────────────────
*/

with

-- ── Loan exposures by Basel class ─────────────────────────────────────────────
loan_exposures as (
    select
        loan_type,
        ifrs9_stage,
        collateral_type,
        ltv_ratio_pct,
        count(loan_id)                                              as exposure_count,
        sum(ead_zar)                                                as ead_zar,
        sum(ecl_amount_zar)                                         as ecl_zar,
        sum(provision_amount_zar)                                   as provision_zar,
        sum(rwa_zar)                                                as rwa_zar,

        -- Basel III exposure class mapping (Standardised Approach)
        case
            when loan_type = 'HOME_LOAN' and ltv_ratio_pct < 80  then 'Residential Mortgage (LTV<80%)'
            when loan_type = 'HOME_LOAN'                          then 'Residential Mortgage (LTV≥80%)'
            when loan_type = 'BUSINESS'                           then 'Corporate Exposure'
            when loan_type = 'PERSONAL'                           then 'Retail Exposure'
            when loan_type = 'VEHICLE'                            then 'Retail Exposure'
            when loan_type = 'CREDIT_CARD'                        then 'Retail Exposure'
            when loan_type = 'OVERDRAFT'                          then 'Retail Exposure'
            else 'Other Exposure'
        end                                                         as basel_exposure_class,

        -- Standardised risk weight (%)
        case
            when loan_type = 'HOME_LOAN' and ltv_ratio_pct < 60  then 35
            when loan_type = 'HOME_LOAN' and ltv_ratio_pct < 80  then 35
            when loan_type = 'HOME_LOAN' and ltv_ratio_pct < 100 then 75
            when loan_type = 'HOME_LOAN'                         then 100
            when loan_type = 'BUSINESS'                          then 100
            when loan_type in ('PERSONAL', 'VEHICLE', 'CREDIT_CARD', 'OVERDRAFT') then 75
            else 100
        end                                                         as std_risk_weight_pct

    from {{ ref('int_loan_health') }}
    where loan_status = 'ACTIVE'
    group by loan_type, ifrs9_stage, collateral_type, ltv_ratio_pct
),

-- ── Deposit liabilities (for LCR / NSFR) ─────────────────────────────────────
deposit_liabilities as (
    select
        account_type,
        sum(current_balance)                                        as deposit_balance_zar,

        -- Runoff rates for LCR (SARB LCR guideline)
        case
            when account_type = 'CHEQUE'        then 0.05  -- stable retail
            when account_type = 'SAVINGS'       then 0.05
            when account_type = 'MONEY_MARKET'  then 0.10  -- less stable
            when account_type = 'NOTICE'        then 0.25  -- wholesale proxy
            when account_type = 'FIXED_DEPOSIT' then 0.00  -- locked; no runoff in 30d
            else 0.10
        end                                                         as lcr_runoff_rate,

        -- Required Stable Funding factor (NSFR)
        case
            when account_type = 'FIXED_DEPOSIT' then 0.95  -- term > 1yr = stable
            when account_type = 'SAVINGS'       then 0.90
            when account_type = 'CHEQUE'        then 0.80
            when account_type = 'MONEY_MARKET'  then 0.50
            else 0.50
        end                                                         as nsfr_asf_factor

    from {{ ref('stg_accounts') }}
    where account_status_derived = 'ACTIVE'
    group by account_type
),

-- ── High-Quality Liquid Assets (HQLA) proxy ───────────────────────────────────
-- In production: sourced from treasury bond/cash positions
-- Here we proxy via RTGS / notice deposit balances
hqla_proxy as (
    select
        -- Level 1 HQLA: cash + central bank reserves (proxy: RTGS settlement balance)
        sum(case when account_type = 'NOTICE'       then current_balance else 0 end) * 0.80
        + sum(case when account_type = 'MONEY_MARKET' then current_balance else 0 end) * 0.50
                                                                    as hqla_level1_zar,
        -- Level 2A: government bonds (proxy: fixed deposits with banks)
        sum(case when account_type = 'FIXED_DEPOSIT' then current_balance else 0 end) * 0.85
                                                                    as hqla_level2a_zar
    from {{ ref('stg_accounts') }}
    where account_status_derived = 'ACTIVE'
),

-- ── Required Stable Funding (RSF) from loan book ──────────────────────────────
rsf_loans as (
    select
        -- RSF factor by remaining maturity (Basel III)
        sum(
            outstanding_principal_zar *
            case
                when remaining_term_months <= 6   then 0.50
                when remaining_term_months <= 12  then 0.85
                else 1.00
            end
        )                                                           as rsf_loans_zar
    from {{ ref('stg_loans') }}
    where loan_status = 'ACTIVE'
),

-- ── RWA roll-up by Basel exposure class ──────────────────────────────────────
rwa_by_class as (
    select
        basel_exposure_class,
        std_risk_weight_pct,
        sum(exposure_count)                                         as exposure_count,
        sum(ead_zar)                                                as ead_zar,
        sum(rwa_zar)                                                as rwa_zar,
        sum(ecl_zar)                                                as ecl_zar,
        sum(provision_zar)                                          as provision_zar
    from loan_exposures
    group by basel_exposure_class, std_risk_weight_pct
),

-- ── Capital ratios (placeholder capital from GL proxy) ────────────────────────
-- In production: equity and deductions from the Balance Sheet GL
capital_positions as (
    select
        -- CET1 = Share capital + Retained earnings − Deductions
        -- Placeholder: 12% of total deposit base (typical SA bank proxy)
        sum(current_balance) * 0.12                                 as cet1_capital_zar,
        sum(current_balance) * 0.02                                 as at1_capital_zar,  -- Additional Tier 1
        sum(current_balance) * 0.02                                 as tier2_capital_zar,
        sum(current_balance)                                        as total_exposure_zar  -- leverage denominator
    from {{ ref('stg_accounts') }}
    where account_status_derived = 'ACTIVE'
),

-- ── Assemble final mart ───────────────────────────────────────────────────────
lcr_nsfr as (
    select
        sum(deposit_balance_zar * lcr_runoff_rate)                  as net_cash_outflows_zar,
        sum(deposit_balance_zar * nsfr_asf_factor)                  as available_stable_funding_zar
    from deposit_liabilities
),

final as (
    select
        rc.basel_exposure_class,
        rc.std_risk_weight_pct,
        rc.exposure_count,
        rc.ead_zar,
        rc.rwa_zar,
        rc.ecl_zar,
        rc.provision_zar,

        -- Capital ratios (attach to each row)
        cp.cet1_capital_zar,
        cp.at1_capital_zar,
        cp.tier2_capital_zar,
        cp.cet1_capital_zar + cp.at1_capital_zar                    as tier1_capital_zar,
        cp.cet1_capital_zar + cp.at1_capital_zar + cp.tier2_capital_zar
                                                                    as total_capital_zar,

        -- Total portfolio RWA (for ratio denominator)
        sum(rc2.rwa_zar) over ()                                    as total_rwa_zar,

        -- Capital ratios (Basel III minimums: CET1 4.5%, Tier1 6%, Total 8%)
        round(cp.cet1_capital_zar / nullif(sum(rc2.rwa_zar) over (), 0) * 100, 4)
                                                                    as cet1_ratio_pct,
        round((cp.cet1_capital_zar + cp.at1_capital_zar) / nullif(sum(rc2.rwa_zar) over (), 0) * 100, 4)
                                                                    as tier1_ratio_pct,
        round((cp.cet1_capital_zar + cp.at1_capital_zar + cp.tier2_capital_zar)
              / nullif(sum(rc2.rwa_zar) over (), 0) * 100, 4)      as total_capital_ratio_pct,

        -- Leverage ratio (Tier 1 / Total Exposure — minimum 3%)
        round((cp.cet1_capital_zar + cp.at1_capital_zar) / nullif(cp.total_exposure_zar, 0) * 100, 4)
                                                                    as leverage_ratio_pct,

        -- LCR (HQLA / NCOF — minimum 100%)
        round(
            (h.hqla_level1_zar + h.hqla_level2a_zar) / nullif(ln.net_cash_outflows_zar, 0) * 100,
        2)                                                          as lcr_pct,

        -- NSFR (ASF / RSF — minimum 100%)
        round(
            ln.available_stable_funding_zar / nullif(rs.rsf_loans_zar, 0) * 100,
        2)                                                          as nsfr_pct,

        -- Compliance flags
        round(cp.cet1_capital_zar / nullif(sum(rc2.rwa_zar) over (), 0) * 100, 4) >= 4.5
                                                                    as cet1_compliant,
        round((cp.cet1_capital_zar + cp.at1_capital_zar) / nullif(sum(rc2.rwa_zar) over (), 0) * 100, 4) >= 6.0
                                                                    as tier1_compliant,
        round((h.hqla_level1_zar + h.hqla_level2a_zar) / nullif(ln.net_cash_outflows_zar, 0) * 100, 2) >= 100
                                                                    as lcr_compliant,

        current_date()                                              as reporting_date,
        current_timestamp()                                         as loaded_at

    from rwa_by_class rc
    cross join rwa_by_class rc2  -- self-cross for window totals (handled by window fn)
    cross join capital_positions cp
    cross join hqla_proxy h
    cross join lcr_nsfr ln
    cross join rsf_loans rs
    qualify row_number() over (partition by rc.basel_exposure_class order by rc.std_risk_weight_pct) = 1
)

select * from final
