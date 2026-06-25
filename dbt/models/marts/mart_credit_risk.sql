{{
    config(
        materialized    = 'table',
        tags            = ['mart', 'credit_risk', 'ifrs9', 'regulatory', 'daily'],
        description     = 'Production credit risk mart: ECL, provisions, NPL rates, IFRS 9 staging, roll rates, vintage curves.',
        partition_by    = {
            'field'       : 'reporting_month',
            'data_type'   : 'date',
            'granularity' : 'month'
        }
    )
}}

/*
  mart_credit_risk
  ─────────────────────────────────────────────────────────────────────────────
  Sources : int_loan_health, stg_loans, int_customer_risk_profile
  Layer   : Gold (mart)
  Grain   : One row per loan (loan_id) × reporting_month
  Purpose : Single authoritative source for the bank's credit portfolio
            dashboard and SARB regulatory submission (BA 100/200 series).
            Covers: ECL, provision, coverage ratio, NPL rate, IFRS 9 staging,
            roll rates, and vintage curve data. Partitioned by reporting_month.
  ─────────────────────────────────────────────────────────────────────────────
*/

with

loan_health as (
    select
        loan_id,
        customer_id,
        account_id,
        branch_id,
        relationship_manager_id,
        loan_type,
        loan_status,
        collateral_type,
        customer_segment,
        employment_status,
        credit_score,
        origination_date,
        vintage_cohort,
        months_on_book,
        maturity_date,
        remaining_term_months,
        roll_rate_bucket,
        delinquency_days,
        delinquency_bucket,
        npl_flag,
        ifrs9_stage,
        original_loan_amount_zar,
        outstanding_principal_zar,
        outstanding_interest_zar,
        outstanding_fees_zar,
        total_outstanding_zar,
        arrears_amount_zar,
        ias39_provisioning_rate,
        provision_amount_zar,
        provision_coverage_pct,
        collateral_value_zar,
        ltv_ratio_pct,
        ltv_band,
        dti_ratio_pct,
        dti_band,
        interest_rate_pct,
        monthly_instalment_zar,
        repayments_made,
        repayments_missed,
        pd_estimate,
        lgd_estimate,
        ead_zar,
        ecl_amount_zar
    from {{ ref('int_loan_health') }}
),

customer_risk as (
    select
        customer_id,
        risk_segment,
        composite_risk_score,
        total_deposit_balance_zar
    from {{ ref('int_customer_risk_profile') }}
),

-- ── Enriched loan with customer risk context ──────────────────────────────────
enriched as (
    select
        l.*,
        coalesce(cr.risk_segment,          'Standard')              as customer_risk_segment,
        coalesce(cr.composite_risk_score,  50)                      as customer_composite_risk_score,
        coalesce(cr.total_deposit_balance_zar, 0)                   as customer_deposit_balance_zar,

        -- ── Reporting month (snapshot date) ───────────────────────────────────
        -- In production, this is parameterised for monthly reruns
        date_trunc('month', current_date())                         as reporting_month,

        -- ── Net Exposure (after collateral netting) ────────────────────────────
        greatest(0,
            total_outstanding_zar - coalesce(collateral_value_zar, 0)
        )                                                           as net_exposure_zar,

        -- ── Provision shortfall (ECL − IAS39 provision) ───────────────────────
        -- Positive = under-provisioned (requires top-up)
        round(ecl_amount_zar - provision_amount_zar, 2)            as provision_shortfall_zar,

        -- ── Collateral coverage ratio ─────────────────────────────────────────
        case
            when total_outstanding_zar > 0
            then round(coalesce(collateral_value_zar, 0) / total_outstanding_zar * 100, 2)
            else null
        end                                                         as collateral_coverage_pct,

        -- ── Loan-to-original ratio (amortisation progress) ───────────────────
        case
            when original_loan_amount_zar > 0
            then round(outstanding_principal_zar / original_loan_amount_zar * 100, 2)
            else null
        end                                                         as outstanding_ratio_pct,

        -- ── Roll-rate direction flag (for MoM roll-rate matrix) ───────────────
        -- This would compare to previous month snapshot; simplified to bucket number
        case roll_rate_bucket
            when 'Current'       then 0
            when '1–30 DPD'      then 1
            when '31–60 DPD'     then 2
            when '61–90 DPD'     then 3
            when 'NPL (>90 DPD)' then 4
            when 'Written-Off'   then 5
            else -1
        end                                                         as roll_rate_order,

        -- ── Capital requirement (simplified Basel III SA approach) ─────────────
        -- RWA = EAD × risk weight; risk weight by rating/LTV
        round(
            ead_zar *
            case
                when loan_type = 'HOME_LOAN'   and ltv_ratio_pct < 80 then 0.35
                when loan_type = 'HOME_LOAN'   and ltv_ratio_pct < 100 then 0.75
                when loan_type = 'HOME_LOAN'                           then 1.00
                when loan_type = 'VEHICLE'                             then 0.75
                when loan_type = 'PERSONAL'                            then 0.75
                when loan_type = 'BUSINESS'    and collateral_type = 'PROPERTY' then 0.50
                when loan_type = 'BUSINESS'                            then 1.00
                when loan_type = 'CREDIT_CARD'                         then 0.75
                else 1.00
            end
        , 2)                                                        as rwa_zar

    from loan_health l
    left join customer_risk cr using (customer_id)
),

-- ── Portfolio-level aggregation columns (window functions) ────────────────────
portfolio_stats as (
    select
        *,
        -- NPL rate = NPL balance / total portfolio (rolling at portfolio level)
        sum(case when npl_flag then total_outstanding_zar else 0 end)
            over ()                                                 as portfolio_npl_balance,
        sum(total_outstanding_zar)
            over ()                                                 as portfolio_total_balance,

        -- Average portfolio LTV
        avg(ltv_ratio_pct)
            over (partition by loan_type)                           as avg_ltv_by_loan_type,

        -- Vintage curve: average DPD by cohort and month on book
        avg(delinquency_days)
            over (partition by vintage_cohort, months_on_book)      as cohort_avg_dpd

    from enriched
),

final as (
    select
        -- Reporting dimension
        reporting_month,

        -- Loan identifiers
        loan_id,
        customer_id,
        account_id,
        branch_id,
        relationship_manager_id,

        -- Loan profile
        loan_type,
        loan_status,
        collateral_type,
        ltv_band,
        dti_band,
        customer_segment,
        customer_risk_segment,
        customer_composite_risk_score,

        -- IFRS 9 classification
        ifrs9_stage,
        roll_rate_bucket,
        roll_rate_order,
        delinquency_days,
        delinquency_bucket,
        npl_flag,
        vintage_cohort,
        months_on_book,

        -- Amounts
        original_loan_amount_zar,
        outstanding_principal_zar,
        outstanding_interest_zar,
        outstanding_fees_zar,
        total_outstanding_zar,
        net_exposure_zar,
        arrears_amount_zar,
        collateral_value_zar,
        collateral_coverage_pct,
        outstanding_ratio_pct,

        -- ECL / provisioning
        pd_estimate,
        lgd_estimate,
        ead_zar,
        ecl_amount_zar,
        ias39_provisioning_rate,
        provision_amount_zar,
        provision_coverage_pct,
        provision_shortfall_zar,

        -- Capital
        rwa_zar,

        -- Portfolio metrics (window)
        portfolio_npl_balance,
        portfolio_total_balance,
        round(portfolio_npl_balance / nullif(portfolio_total_balance, 0) * 100, 4)
                                                                    as portfolio_npl_rate_pct,
        avg_ltv_by_loan_type,
        cohort_avg_dpd,

        -- Rates
        interest_rate_pct,

        current_timestamp()                                         as loaded_at

    from portfolio_stats
)

select * from final
