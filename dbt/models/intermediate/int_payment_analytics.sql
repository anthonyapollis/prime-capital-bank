{{
    config(
        materialized = 'table',
        tags         = ['intermediate', 'payments', 'treasury', 'daily'],
        description  = 'Payment analytics: rail mix, settlement time, failure rates, and correspondent bank concentration.'
    )
}}

/*
  int_payment_analytics
  ─────────────────────────────────────────────────────────────────────────────
  Sources : stg_payments, stg_accounts
  Layer   : Intermediate
  Grain   : One row per account (account_id), summarising rolling 12-month
            payment behaviour
  Purpose : Analyse payment rail mix, average settlement lag, failure rates,
            and concentration risk by correspondent bank. Used by
            mart_treasury_analytics and operations dashboards.
  ─────────────────────────────────────────────────────────────────────────────
*/

with

payments as (
    select
        payment_id,
        account_id,
        customer_id,
        payment_date,
        payment_rail,
        payment_status,
        is_failed,
        is_cross_border,
        is_sla_breach,
        payment_amount_zar,
        payment_fee_zar,
        settlement_lag_days,
        expected_settlement_days,
        beneficiary_bank_code,
        beneficiary_country_code,
        payment_purpose,
        failure_reason_code
    from {{ ref('stg_payments') }}
    where payment_date >= date_add(current_date(), -365)
),

accounts as (
    select
        account_id,
        customer_id,
        branch_id,
        account_type,
        account_status_derived,
        current_balance
    from {{ ref('stg_accounts') }}
),

-- ── Payment summary per account ────────────────────────────────────────────────
payment_summary as (
    select
        account_id,

        -- Volume & value
        count(payment_id)                                           as total_payments_12m,
        sum(payment_amount_zar)                                     as total_payment_value_zar,
        avg(payment_amount_zar)                                     as avg_payment_value_zar,
        sum(payment_fee_zar)                                        as total_payment_fees_zar,

        -- Rail mix
        count(case when payment_rail = 'EFT'      then 1 end)       as eft_count,
        count(case when payment_rail = 'RTGS'     then 1 end)       as rtgs_count,
        count(case when payment_rail = 'SWIFT'    then 1 end)       as swift_count,
        count(case when payment_rail = 'Internal' then 1 end)       as internal_count,

        sum(case when payment_rail = 'EFT'      then payment_amount_zar else 0 end) as eft_value_zar,
        sum(case when payment_rail = 'RTGS'     then payment_amount_zar else 0 end) as rtgs_value_zar,
        sum(case when payment_rail = 'SWIFT'    then payment_amount_zar else 0 end) as swift_value_zar,
        sum(case when payment_rail = 'Internal' then payment_amount_zar else 0 end) as internal_value_zar,

        -- Cross-border
        count(case when is_cross_border = true    then 1 end)       as cross_border_count,
        sum(case when is_cross_border = true then payment_amount_zar else 0 end)
                                                                    as cross_border_value_zar,

        -- Settlement performance
        avg(case when settlement_lag_days is not null then settlement_lag_days end)
                                                                    as avg_settlement_lag_days,
        percentile_approx(coalesce(settlement_lag_days, 0), 0.90)  as p90_settlement_lag_days,
        count(case when is_sla_breach = true then 1 end)            as sla_breach_count,

        -- Failure
        count(case when is_failed = true          then 1 end)       as failed_payment_count,
        sum(case when is_failed = true then payment_amount_zar else 0 end)
                                                                    as failed_payment_value_zar,

        -- Purpose mix
        count(case when payment_purpose = 'SALARY'    then 1 end)   as salary_payment_count,
        count(case when payment_purpose = 'SUPPLIER'  then 1 end)   as supplier_payment_count,
        count(case when payment_purpose = 'LOAN_REPAYMENT' then 1 end) as loan_repayment_count

    from payments
    group by account_id
),

-- ── Correspondent bank concentration per account ──────────────────────────────
-- Top beneficiary bank by payment count (concentration risk indicator)
bank_concentration as (
    select
        account_id,
        beneficiary_bank_code                                       as top_beneficiary_bank,
        count(payment_id)                                           as payments_to_top_bank,
        rank() over (
            partition by account_id
            order by count(payment_id) desc
        )                                                           as bank_rank
    from payments
    where beneficiary_bank_code is not null
    group by account_id, beneficiary_bank_code
),

top_bank as (
    select account_id, top_beneficiary_bank, payments_to_top_bank
    from bank_concentration
    where bank_rank = 1
),

-- ── Most common failure reason per account ────────────────────────────────────
failure_mode as (
    select
        account_id,
        failure_reason_code                                         as primary_failure_reason,
        count(payment_id)                                           as failure_count,
        rank() over (
            partition by account_id
            order by count(payment_id) desc
        )                                                           as failure_rank
    from payments
    where is_failed = true
      and failure_reason_code is not null
    group by account_id, failure_reason_code
),

top_failure as (
    select account_id, primary_failure_reason
    from failure_mode
    where failure_rank = 1
),

-- ── Join onto account spine ────────────────────────────────────────────────────
combined as (
    select
        a.account_id,
        a.customer_id,
        a.branch_id,
        a.account_type,
        a.account_status_derived,
        a.current_balance,

        coalesce(ps.total_payments_12m,        0)                   as total_payments_12m,
        coalesce(ps.total_payment_value_zar,   0)                   as total_payment_value_zar,
        coalesce(ps.avg_payment_value_zar,     0)                   as avg_payment_value_zar,
        coalesce(ps.total_payment_fees_zar,    0)                   as total_payment_fees_zar,

        coalesce(ps.eft_count,      0)                              as eft_count,
        coalesce(ps.rtgs_count,     0)                              as rtgs_count,
        coalesce(ps.swift_count,    0)                              as swift_count,
        coalesce(ps.internal_count, 0)                              as internal_count,

        coalesce(ps.eft_value_zar,      0)                          as eft_value_zar,
        coalesce(ps.rtgs_value_zar,     0)                          as rtgs_value_zar,
        coalesce(ps.swift_value_zar,    0)                          as swift_value_zar,
        coalesce(ps.internal_value_zar, 0)                          as internal_value_zar,

        coalesce(ps.cross_border_count,      0)                     as cross_border_count,
        coalesce(ps.cross_border_value_zar,  0)                     as cross_border_value_zar,
        coalesce(ps.avg_settlement_lag_days, 0)                     as avg_settlement_lag_days,
        coalesce(ps.p90_settlement_lag_days, 0)                     as p90_settlement_lag_days,
        coalesce(ps.sla_breach_count,        0)                     as sla_breach_count,
        coalesce(ps.failed_payment_count,    0)                     as failed_payment_count,
        coalesce(ps.failed_payment_value_zar, 0)                    as failed_payment_value_zar,
        coalesce(ps.salary_payment_count,    0)                     as salary_payment_count,
        coalesce(ps.supplier_payment_count,  0)                     as supplier_payment_count,
        coalesce(ps.loan_repayment_count,    0)                     as loan_repayment_count,

        tb.top_beneficiary_bank,
        coalesce(tb.payments_to_top_bank,    0)                     as payments_to_top_bank,
        tf.primary_failure_reason

    from accounts a
    left join payment_summary ps using (account_id)
    left join top_bank        tb using (account_id)
    left join top_failure     tf using (account_id)
),

-- ── Derived ratios ─────────────────────────────────────────────────────────────
final_enriched as (
    select
        *,

        -- Payment failure rate
        case
            when total_payments_12m > 0
            then round(failed_payment_count / total_payments_12m * 100, 2)
            else 0
        end                                                         as payment_failure_rate_pct,

        -- SLA breach rate
        case
            when total_payments_12m > 0
            then round(sla_breach_count / total_payments_12m * 100, 2)
            else 0
        end                                                         as sla_breach_rate_pct,

        -- Correspondent bank concentration (% to top bank)
        case
            when total_payments_12m > 0
            then round(payments_to_top_bank / total_payments_12m * 100, 2)
            else 0
        end                                                         as top_bank_concentration_pct,

        -- Dominant rail by volume
        case
            when greatest(eft_count, rtgs_count, swift_count, internal_count) = eft_count      then 'EFT'
            when greatest(eft_count, rtgs_count, swift_count, internal_count) = rtgs_count     then 'RTGS'
            when greatest(eft_count, rtgs_count, swift_count, internal_count) = swift_count    then 'SWIFT'
            else 'Internal'
        end                                                         as dominant_payment_rail,

        current_timestamp()                                         as loaded_at

    from combined
)

select * from final_enriched
