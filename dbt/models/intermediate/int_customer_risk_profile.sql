{{
    config(
        materialized = 'table',
        tags         = ['intermediate', 'credit_risk', 'customer', 'daily'],
        description  = 'Customer composite risk profile: joins customer, account, loan, fraud, and AML data to produce a risk segment.'
    )
}}

/*
  int_customer_risk_profile
  ─────────────────────────────────────────────────────────────────────────────
  Sources : stg_customers, stg_accounts, stg_loans, stg_fraud_alerts,
            stg_aml_alerts
  Layer   : Intermediate
  Grain   : One row per customer (customer_id)
  Purpose : Build a customer-level composite risk score by aggregating credit
            quality, delinquency history, fraud incidents, and AML risk signals.
            Output risk_segment: Prime / Standard / Substandard / Doubtful / Loss
            (aligned with NCR and SARB classification guidance).
  ─────────────────────────────────────────────────────────────────────────────
*/

with

customers as (
    select
        customer_id,
        full_name,
        age_years,
        customer_segment,
        annual_income_zar,
        income_band,
        credit_score,
        kyc_complete,
        is_politically_exposed,
        is_sanctioned,
        is_active,
        tenure_days
    from {{ ref('stg_customers') }}
    where is_active = true
),

-- ── Account aggregates per customer ───────────────────────────────────────────
account_agg as (
    select
        customer_id,
        count(account_id)                                           as total_accounts,
        count(case when account_type = 'CHEQUE'       then 1 end)  as cheque_accounts,
        count(case when account_type = 'SAVINGS'      then 1 end)  as savings_accounts,
        count(case when is_dormant = true             then 1 end)  as dormant_accounts,
        sum(current_balance)                                        as total_deposit_balance_zar,
        max(current_balance)                                        as max_account_balance_zar,
        count(case when account_status_derived = 'FROZEN' then 1 end) as frozen_accounts
    from {{ ref('stg_accounts') }}
    group by customer_id
),

-- ── Loan aggregates per customer ───────────────────────────────────────────────
loan_agg as (
    select
        customer_id,
        count(loan_id)                                              as total_loans,
        sum(outstanding_principal_zar)                             as total_loan_exposure_zar,
        sum(arrears_amount_zar)                                    as total_arrears_zar,
        max(delinquency_days)                                      as max_delinquency_days,
        count(case when npl_flag = true  then 1 end)               as npl_loan_count,
        count(case when ifrs9_stage = 'Stage 3' then 1 end)        as stage3_loan_count,
        sum(provision_amount_zar)                                  as total_provision_zar,
        -- Worst IFRS9 stage across all loans
        max(case
            when ifrs9_stage = 'Stage 3' then 3
            when ifrs9_stage = 'Stage 2' then 2
            else 1
        end)                                                        as worst_ifrs9_stage_num
    from {{ ref('stg_loans') }}
    where loan_status not in ('SETTLED', 'WRITTEN_OFF')
    group by customer_id
),

-- ── Fraud history per customer ────────────────────────────────────────────────
fraud_agg as (
    select
        customer_id,
        count(alert_id)                                             as total_fraud_alerts,
        count(case when is_confirmed_fraud = true then 1 end)      as confirmed_fraud_count,
        sum(case when is_confirmed_fraud = true then net_loss_zar else 0 end)
                                                                    as total_fraud_loss_zar,
        max(fraud_score)                                            as max_fraud_score,
        -- Has open/unresolved fraud alert?
        max(case when alert_status = 'OPEN' then 1 else 0 end)     as has_open_fraud_alert
    from {{ ref('stg_fraud_alerts') }}
    group by customer_id
),

-- ── AML risk per customer ──────────────────────────────────────────────────────
aml_agg as (
    select
        customer_id,
        count(aml_alert_id)                                         as total_aml_alerts,
        max(composite_risk_score)                                   as max_aml_risk_score,
        max(case when sar_candidate_flag = true then 1 else 0 end)  as has_sar_candidate,
        max(case when str_filed = true         then 1 else 0 end)   as has_filed_str,
        max(case when customer_is_sanctioned   then 1 else 0 end)   as is_sanctioned_flag
    from {{ ref('stg_aml_alerts') }}
    group by customer_id
),

-- ── Join all domains onto customer spine ──────────────────────────────────────
joined as (
    select
        c.*,
        coalesce(a.total_accounts,          0)                      as total_accounts,
        coalesce(a.total_deposit_balance_zar, 0)                    as total_deposit_balance_zar,
        coalesce(a.dormant_accounts,        0)                      as dormant_accounts,
        coalesce(a.frozen_accounts,         0)                      as frozen_accounts,

        coalesce(l.total_loans,             0)                      as total_loans,
        coalesce(l.total_loan_exposure_zar, 0)                      as total_loan_exposure_zar,
        coalesce(l.total_arrears_zar,       0)                      as total_arrears_zar,
        coalesce(l.max_delinquency_days,    0)                      as max_delinquency_days,
        coalesce(l.npl_loan_count,          0)                      as npl_loan_count,
        coalesce(l.stage3_loan_count,       0)                      as stage3_loan_count,
        coalesce(l.total_provision_zar,     0)                      as total_provision_zar,
        coalesce(l.worst_ifrs9_stage_num,   1)                      as worst_ifrs9_stage_num,

        coalesce(f.total_fraud_alerts,      0)                      as total_fraud_alerts,
        coalesce(f.confirmed_fraud_count,   0)                      as confirmed_fraud_count,
        coalesce(f.total_fraud_loss_zar,    0)                      as total_fraud_loss_zar,
        coalesce(f.max_fraud_score,         0)                      as max_fraud_score,
        coalesce(f.has_open_fraud_alert,    0)                      as has_open_fraud_alert,

        coalesce(am.total_aml_alerts,       0)                      as total_aml_alerts,
        coalesce(am.max_aml_risk_score,     0)                      as max_aml_risk_score,
        coalesce(am.has_sar_candidate,      0)                      as has_sar_candidate,
        coalesce(am.has_filed_str,          0)                      as has_filed_str,
        coalesce(am.is_sanctioned_flag,     0)                      as is_sanctioned_aml_flag

    from customers c
    left join account_agg a  using (customer_id)
    left join loan_agg    l  using (customer_id)
    left join fraud_agg   f  using (customer_id)
    left join aml_agg     am using (customer_id)
),

-- ── Composite risk score (0–100, lower = better) ──────────────────────────────
scored as (
    select
        *,

        -- Credit sub-score (40 pts): based on credit_score (300–850) and delinquency
        round(
            -- Credit bureau score contribution (inverted: 850=0 pts, 300=40 pts)
            greatest(0, least(40, (850 - coalesce(credit_score, 600)) / 550.0 * 25))
            -- Delinquency penalty
            + case
                when max_delinquency_days > 90   then 15
                when max_delinquency_days > 60   then 10
                when max_delinquency_days > 30   then 5
                else 0
              end
        , 2)                                                        as credit_sub_score,

        -- Fraud sub-score (25 pts)
        round(
            least(25,
                (confirmed_fraud_count * 10)
                + (has_open_fraud_alert * 8)
                + (max_fraud_score / 100.0 * 7)
            )
        , 2)                                                        as fraud_sub_score,

        -- AML sub-score (25 pts)
        round(
            least(25,
                (has_sar_candidate * 10)
                + (is_sanctioned_aml_flag * 15)
                + (has_filed_str * 5)
                + (max_aml_risk_score / 100.0 * 5)
            )
        , 2)                                                        as aml_sub_score,

        -- PEP / sanctions hard penalties (10 pts)
        (case when is_politically_exposed then 5 else 0 end)
        + (case when is_sanctioned        then 10 else 0 end)      as pep_sanction_penalty

    from joined
),

risk_segmented as (
    select
        *,

        -- Total composite risk score
        least(100,
            credit_sub_score + fraud_sub_score + aml_sub_score + pep_sanction_penalty
        )                                                           as composite_risk_score,

        -- ── Risk segment (aligned to SARB / NCR classification) ────────────────
        {{ assign_risk_segment(
            'least(100, credit_sub_score + fraud_sub_score + aml_sub_score + pep_sanction_penalty)',
            'npl_loan_count',
            'is_sanctioned'
        ) }}                                                        as risk_segment

    from scored
),

final as (
    select
        customer_id,
        full_name,
        age_years,
        customer_segment,
        annual_income_zar,
        income_band,
        credit_score,
        kyc_complete,
        is_politically_exposed,
        is_sanctioned,
        tenure_days,
        total_accounts,
        total_deposit_balance_zar,
        dormant_accounts,
        frozen_accounts,
        total_loans,
        total_loan_exposure_zar,
        total_arrears_zar,
        max_delinquency_days,
        npl_loan_count,
        stage3_loan_count,
        total_provision_zar,
        worst_ifrs9_stage_num,
        total_fraud_alerts,
        confirmed_fraud_count,
        total_fraud_loss_zar,
        max_fraud_score,
        has_open_fraud_alert,
        total_aml_alerts,
        max_aml_risk_score,
        has_sar_candidate,
        has_filed_str,
        is_sanctioned_aml_flag,
        credit_sub_score,
        fraud_sub_score,
        aml_sub_score,
        pep_sanction_penalty,
        composite_risk_score,
        risk_segment,
        current_timestamp()                                         as loaded_at
    from risk_segmented
)

select * from final
