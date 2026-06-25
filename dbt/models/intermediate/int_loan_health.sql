{{
    config(
        materialized = 'table',
        tags         = ['intermediate', 'credit_risk', 'loans', 'daily'],
        description  = 'Loan health analytics: LTV, DTI, vintage, roll-rate buckets, and IFRS 9 ECL computation.'
    )
}}

/*
  int_loan_health
  ─────────────────────────────────────────────────────────────────────────────
  Sources : stg_loans, stg_customers
  Layer   : Intermediate
  Grain   : One row per loan (loan_id)
  Purpose : Enriches each loan with customer income data to compute DTI,
            derives LTV, maps to roll-rate buckets (Current / 1-30 DPD /
            31-60 DPD / 61-90 DPD / NPL), assigns vintage cohorts, and
            computes Expected Credit Loss (ECL = PD × LGD × EAD) per IFRS 9.
  ─────────────────────────────────────────────────────────────────────────────
*/

with

loans as (
    select
        loan_id,
        customer_id,
        account_id,
        branch_id,
        relationship_manager_id,
        loan_type,
        loan_status,
        collateral_type,
        origination_date,
        maturity_date,
        delinquency_days,
        delinquency_bucket,
        npl_flag,
        ifrs9_stage,
        ias39_provisioning_rate,
        original_loan_amount_zar,
        outstanding_principal_zar,
        outstanding_interest_zar,
        outstanding_fees_zar,
        total_outstanding_zar,
        arrears_amount_zar,
        provision_amount_zar,
        collateral_value_zar,
        ltv_ratio_pct,
        interest_rate_pct,
        monthly_instalment_zar,
        loan_term_months,
        remaining_term_months,
        repayments_made,
        repayments_missed,
        vintage_month
    from {{ ref('stg_loans') }}
),

customers as (
    select
        customer_id,
        annual_income_zar,
        credit_score,
        employment_status,
        customer_segment
    from {{ ref('stg_customers') }}
),

-- ── Join customer income for DTI calculation ──────────────────────────────────
loan_with_customer as (
    select
        l.*,
        coalesce(c.annual_income_zar, 0)                            as annual_income_zar,
        coalesce(c.credit_score,      600)                          as credit_score,
        c.employment_status,
        c.customer_segment
    from loans l
    left join customers c using (customer_id)
),

-- ── Roll-rate bucket & vintage ────────────────────────────────────────────────
enriched as (
    select
        *,

        -- ── Debt-to-Income ratio ─────────────────────────────────────────────
        -- Monthly instalment as % of monthly income
        case
            when annual_income_zar > 0
            then round(
                    (monthly_instalment_zar / (annual_income_zar / 12)) * 100,
                 2)
            else null
        end                                                         as dti_ratio_pct,

        -- ── DTI band ─────────────────────────────────────────────────────────
        case
            when annual_income_zar <= 0                             then 'No Income Data'
            when (monthly_instalment_zar / (annual_income_zar / 12)) <= 0.30  then 'Low (<30%)'
            when (monthly_instalment_zar / (annual_income_zar / 12)) <= 0.43  then 'Moderate (30–43%)'
            when (monthly_instalment_zar / (annual_income_zar / 12)) <= 0.60  then 'High (43–60%)'
            else                                                         'Very High (>60%)'
        end                                                         as dti_band,

        -- ── Roll-rate bucket ─────────────────────────────────────────────────
        case
            when loan_status in ('WRITTEN_OFF')                     then 'Written-Off'
            when delinquency_days = 0                               then 'Current'
            when delinquency_days between 1  and 30                 then '1–30 DPD'
            when delinquency_days between 31 and 60                 then '31–60 DPD'
            when delinquency_days between 61 and 90                 then '61–90 DPD'
            else                                                         'NPL (>90 DPD)'
        end                                                         as roll_rate_bucket,

        -- ── Vintage cohort (YYYY-MM of origination) ───────────────────────────
        date_format(origination_date, 'yyyy-MM')                    as vintage_cohort,

        -- Months on book
        floor(datediff(current_date(), origination_date) / 30.44)  as months_on_book,

        -- ── LTV band ─────────────────────────────────────────────────────────
        case
            when collateral_value_zar is null or collateral_value_zar = 0  then 'Unsecured'
            when ltv_ratio_pct < 60    then 'Low LTV (<60%)'
            when ltv_ratio_pct < 80    then 'Standard LTV (60–80%)'
            when ltv_ratio_pct < 100   then 'High LTV (80–100%)'
            else                            'Over-Secured (>100%)'
        end                                                         as ltv_band

    from loan_with_customer
),

-- ── IFRS 9 ECL components ─────────────────────────────────────────────────────
ecl_computed as (
    select
        *,

        -- ── Probability of Default (PD) — simplified scorecard approach ───────
        -- Uses credit score (300–850) and delinquency to derive annual PD
        round(
            case
                when ifrs9_stage = 'Stage 3' then 1.00   -- certain default
                when ifrs9_stage = 'Stage 2'
                then greatest(0.05,
                        least(0.80,
                            (1 - (credit_score - 300) / 550.0) * 0.60
                            + (delinquency_days / 90.0) * 0.40
                        ))
                -- Stage 1: 12-month PD from credit score
                else greatest(0.001,
                        (1 - (credit_score - 300) / 550.0) * 0.15
                     )
            end
        , 4)                                                        as pd_estimate,

        -- ── Loss Given Default (LGD) — function of collateral coverage ────────
        round(
            case
                when collateral_type = 'UNSECURED'  then 0.75
                when collateral_type = 'PROPERTY'   then
                    greatest(0.05, least(0.50,
                        1 - (coalesce(collateral_value_zar, 0) / greatest(total_outstanding_zar, 1))
                    ))
                when collateral_type = 'VEHICLE'    then 0.45
                when collateral_type = 'SURETY'     then 0.55
                when collateral_type = 'CESSION'    then 0.35
                else {{ var('default_lgd') }}
            end
        , 4)                                                        as lgd_estimate,

        -- ── Exposure at Default (EAD) — outstanding balance + undrawn factor ──
        round(
            total_outstanding_zar * {{ var('default_ead_factor') }},
        2)                                                          as ead_zar

    from enriched
),

ecl_final as (
    select
        *,

        -- ── Expected Credit Loss = PD × LGD × EAD ────────────────────────────
        -- Stage 1: 12-month ECL
        -- Stage 2 & 3: lifetime ECL (use remaining term factor)
        round(
            pd_estimate * lgd_estimate * ead_zar *
            case
                when ifrs9_stage = 'Stage 1' then 1.0  -- 12-month PD already
                else greatest(1.0, remaining_term_months / 12.0)   -- lifetime
            end
        , 2)                                                        as ecl_amount_zar,

        -- Coverage ratio = provision / outstanding principal
        case
            when outstanding_principal_zar > 0
            then round(provision_amount_zar / outstanding_principal_zar * 100, 2)
            else null
        end                                                         as provision_coverage_pct

    from ecl_computed
),

final as (
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
        vintage_month,
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
        annual_income_zar,
        dti_ratio_pct,
        dti_band,
        interest_rate_pct,
        monthly_instalment_zar,
        repayments_made,
        repayments_missed,
        pd_estimate,
        lgd_estimate,
        ead_zar,
        ecl_amount_zar,
        current_timestamp()                                         as loaded_at
    from ecl_final
)

select * from final
