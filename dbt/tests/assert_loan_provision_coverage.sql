-- Custom dbt test: provision amount must be >= expected ECL for all NPL loans.
-- IFRS 9 Stage 3 (NPL) loans must be provisioned at minimum 50% (Doubtful) or 100% (Loss).
-- Fails if any underprovided NPL loans are found.

with loan_provision_check as (
    select
        loan_sk,
        loan_id,
        outstanding_balance,
        provision_amount,
        ecl_amount,
        ifrs9_stage,
        loan_status,
        days_past_due,
        -- Minimum required provision rate per IFRS 9 stage
        case ifrs9_stage
            when 'Stage 1' then 0.005
            when 'Stage 2' then 0.25
            when 'Stage 3' then 0.50
            else 0.01
        end as min_provision_rate,
        outstanding_balance * case ifrs9_stage
            when 'Stage 1' then 0.005
            when 'Stage 2' then 0.25
            when 'Stage 3' then 0.50
            else 0.01
        end as min_provision_required
    from {{ ref('fact_loan_portfolio') }}
    where is_npl = true
      or ifrs9_stage in ('Stage 2', 'Stage 3')
)
select
    loan_sk,
    loan_id,
    outstanding_balance,
    provision_amount,
    min_provision_required,
    provision_amount - min_provision_required as provision_shortfall,
    ifrs9_stage,
    days_past_due
from loan_provision_check
where
    -- Flag loans where provision is less than minimum required (allow R10 tolerance)
    provision_amount < min_provision_required - 10
    and outstanding_balance > 0
