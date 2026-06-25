{{
    config(
        materialized = 'table',
        tags         = ['mart', 'customer', 'crm', 'daily'],
        description  = 'Single customer view: demographics, all products, assets, liabilities, NII contribution, risk rating, churn probability.'
    )
}}

/*
  mart_customer_360
  ─────────────────────────────────────────────────────────────────────────────
  Sources : stg_customers, int_customer_risk_profile, int_account_profitability,
            stg_loans, stg_credit_cards, int_payment_analytics
  Layer   : Gold (mart)
  Grain   : One row per customer (customer_id)
  Purpose : 360° customer view powering the CRM, Relationship Manager
            dashboards, and the ML churn/CLV feature store. Contains
            demographics, all products held, total assets & liabilities with
            the bank, NII contribution, risk rating, digital engagement score,
            and a simplified churn probability score.
  ─────────────────────────────────────────────────────────────────────────────
*/

with

customers as (
    select
        customer_id,
        full_name,
        first_name,
        last_name,
        date_of_birth,
        age_years,
        age_band,
        gender,
        nationality,
        id_type,
        id_number,
        email_address,
        phone_number,
        city,
        province,
        postal_code,
        country_code,
        customer_since_date,
        tenure_days,
        customer_segment,
        kyc_status,
        kyc_complete,
        employment_status,
        annual_income_zar,
        income_band,
        credit_score,
        is_politically_exposed,
        is_sanctioned,
        is_active
    from {{ ref('stg_customers') }}
    where is_active = true
),

risk_profile as (
    select
        customer_id,
        risk_segment,
        composite_risk_score,
        total_accounts,
        total_deposit_balance_zar,
        total_loans,
        total_loan_exposure_zar,
        total_arrears_zar,
        max_delinquency_days,
        npl_loan_count,
        confirmed_fraud_count,
        total_fraud_loss_zar,
        has_open_fraud_alert,
        total_aml_alerts,
        has_sar_candidate
    from {{ ref('int_customer_risk_profile') }}
),

-- ── Account profitability aggregated to customer level ────────────────────────
account_prof as (
    select
        customer_id,
        count(account_id)                                           as product_account_count,
        sum(nim_zar)                                                as total_nim_zar,
        sum(total_fee_income_zar)                                   as total_fee_income_zar,
        sum(net_profit_zar)                                         as total_net_profit_zar,
        sum(cost_to_serve_zar)                                      as total_cost_to_serve_zar,
        avg(digital_engagement_pct)                                 as avg_digital_engagement_pct,
        sum(txn_count_12m)                                          as total_txn_count_12m,
        sum(gross_credits_zar)                                      as total_credits_zar,
        sum(gross_debits_zar)                                       as total_debits_zar
    from {{ ref('int_account_profitability') }}
    group by customer_id
),

-- ── Loan aggregates per customer ───────────────────────────────────────────────
loan_agg as (
    select
        customer_id,
        count(distinct loan_type)                                   as loan_product_types,
        sum(original_loan_amount_zar)                               as total_loans_originated_zar,
        sum(outstanding_principal_zar)                              as total_loan_balance_zar,
        sum(ecl_amount_zar)                                         as total_ecl_zar,
        max(ltv_ratio_pct)                                          as max_ltv_pct,
        min(credit_score)                                           as credit_score_at_origination,
        count(case when loan_type = 'HOME_LOAN'    then 1 end)      as home_loans,
        count(case when loan_type = 'VEHICLE'      then 1 end)      as vehicle_loans,
        count(case when loan_type = 'PERSONAL'     then 1 end)      as personal_loans,
        count(case when loan_type = 'BUSINESS'     then 1 end)      as business_loans
    from {{ ref('stg_loans') }}
    where loan_status not in ('SETTLED', 'WRITTEN_OFF')
    group by customer_id
),

-- ── Credit card aggregates per customer ───────────────────────────────────────
cc_agg as (
    select
        customer_id,
        count(card_id)                                              as credit_card_count,
        sum(credit_limit_zar)                                       as total_credit_limit_zar,
        sum(current_balance_zar)                                    as total_card_balance_zar,
        max(utilisation_rate_pct)                                   as max_card_utilisation_pct,
        sum(rewards_points_balance)                                 as total_rewards_points,
        max(reward_tier)                                            as highest_reward_tier,
        count(case when missed_payment_flag = true then 1 end)      as missed_cc_payments,
        count(case when is_overlimit         = true then 1 end)     as overlimit_cards
    from {{ ref('stg_credit_cards') }}
    where card_status = 'ACTIVE'
    group by customer_id
),

-- ── Payment behaviour ──────────────────────────────────────────────────────────
payment_behav as (
    select
        customer_id,
        sum(total_payments_12m)                                     as total_outgoing_payments_12m,
        sum(total_payment_value_zar)                                as total_outgoing_value_zar,
        max(avg_settlement_lag_days)                                as max_settlement_lag,
        avg(payment_failure_rate_pct)                               as avg_payment_failure_rate_pct
    from {{ ref('int_payment_analytics') }}
    group by customer_id
),

-- ── Join all domains ──────────────────────────────────────────────────────────
combined as (
    select
        c.*,

        -- Risk
        coalesce(rp.risk_segment,                'Standard')        as risk_segment,
        coalesce(rp.composite_risk_score,        50)                as composite_risk_score,
        coalesce(rp.total_accounts,              0)                 as total_accounts,
        coalesce(rp.total_deposit_balance_zar,   0)                 as total_deposit_balance_zar,
        coalesce(rp.total_loans,                 0)                 as total_loans,
        coalesce(rp.total_loan_exposure_zar,     0)                 as total_loan_exposure_zar,
        coalesce(rp.total_arrears_zar,           0)                 as total_arrears_zar,
        coalesce(rp.max_delinquency_days,        0)                 as max_delinquency_days,
        coalesce(rp.npl_loan_count,              0)                 as npl_loan_count,
        coalesce(rp.confirmed_fraud_count,       0)                 as confirmed_fraud_count,
        coalesce(rp.total_fraud_loss_zar,        0)                 as total_fraud_loss_zar,
        coalesce(rp.has_open_fraud_alert,        0)                 as has_open_fraud_alert,

        -- Profitability
        coalesce(ap.total_nim_zar,               0)                 as total_nim_zar,
        coalesce(ap.total_fee_income_zar,        0)                 as total_fee_income_zar,
        coalesce(ap.total_net_profit_zar,        0)                 as total_net_profit_zar,
        coalesce(ap.total_cost_to_serve_zar,     0)                 as total_cost_to_serve_zar,
        coalesce(ap.avg_digital_engagement_pct,  0)                 as digital_engagement_pct,
        coalesce(ap.total_txn_count_12m,         0)                 as total_txn_count_12m,

        -- Loans
        coalesce(la.loan_product_types,          0)                 as loan_product_types,
        coalesce(la.total_loans_originated_zar,  0)                 as total_loans_originated_zar,
        coalesce(la.total_loan_balance_zar,      0)                 as total_loan_balance_zar,
        coalesce(la.total_ecl_zar,               0)                 as total_ecl_zar,
        coalesce(la.home_loans,                  0)                 as home_loans,
        coalesce(la.vehicle_loans,               0)                 as vehicle_loans,
        coalesce(la.personal_loans,              0)                 as personal_loans,

        -- Credit cards
        coalesce(cc.credit_card_count,           0)                 as credit_card_count,
        coalesce(cc.total_credit_limit_zar,      0)                 as total_credit_limit_zar,
        coalesce(cc.total_card_balance_zar,      0)                 as total_card_balance_zar,
        coalesce(cc.total_rewards_points,        0)                 as total_rewards_points,
        coalesce(cc.highest_reward_tier,         'Standard')        as highest_reward_tier,

        -- Payments
        coalesce(pb.total_outgoing_payments_12m, 0)                 as outgoing_payments_12m,
        coalesce(pb.avg_payment_failure_rate_pct,0)                 as avg_payment_failure_rate_pct

    from customers c
    left join risk_profile  rp using (customer_id)
    left join account_prof  ap using (customer_id)
    left join loan_agg      la using (customer_id)
    left join cc_agg        cc using (customer_id)
    left join payment_behav pb using (customer_id)
),

-- ── Derived metrics ───────────────────────────────────────────────────────────
enriched as (
    select
        *,

        -- Total assets with bank
        round(total_deposit_balance_zar + total_card_balance_zar, 2)  as total_assets_zar,

        -- Total liabilities with bank
        round(total_loan_balance_zar + total_card_balance_zar, 2)     as total_liabilities_zar,

        -- Net position
        round(total_deposit_balance_zar - total_loan_balance_zar, 2)  as net_balance_zar,

        -- Products held count
        (case when total_accounts       > 0 then 1 else 0 end)
        + (case when total_loans        > 0 then 1 else 0 end)
        + (case when credit_card_count  > 0 then 1 else 0 end)        as products_held,

        -- ── Simplified churn probability score (0–100, higher = more likely to churn) ──
        -- Churn signals: low tenure, dormant accounts, no recent txns, low engagement
        least(100,
            -- Short tenure penalty
            greatest(0, (365 - least(tenure_days, 365)) / 365.0 * 20)
            -- Low transaction activity
            + case when total_txn_count_12m < 10 then 15
                   when total_txn_count_12m < 30 then 8
                   else 0 end
            -- Low digital engagement
            + case when digital_engagement_pct < 10 then 10
                   when digital_engagement_pct < 30 then 5
                   else 0 end
            -- Only one product (low stickiness)
            + case when products_held <= 1 then 15 else 0 end
            -- Payment failures (frustration signal)
            + (avg_payment_failure_rate_pct / 100.0 * 10)
            -- Delinquency (at-risk customer)
            + case when max_delinquency_days > 60 then 10
                   when max_delinquency_days > 30 then 5
                   else 0 end
        )                                                             as churn_probability_score

    from combined
),

final as (
    select
        customer_id,
        full_name,
        first_name,
        last_name,
        date_of_birth,
        age_years,
        age_band,
        gender,
        nationality,
        id_type,
        email_address,
        phone_number,
        city,
        province,
        country_code,
        customer_since_date,
        tenure_days,
        customer_segment,
        kyc_status,
        kyc_complete,
        employment_status,
        annual_income_zar,
        income_band,
        credit_score,
        is_politically_exposed,
        is_sanctioned,
        risk_segment,
        composite_risk_score,
        churn_probability_score,

        -- Products held
        products_held,
        total_accounts,
        total_loans,
        loan_product_types,
        credit_card_count,
        home_loans,
        vehicle_loans,
        personal_loans,
        highest_reward_tier,
        total_rewards_points,

        -- Financials
        total_assets_zar,
        total_liabilities_zar,
        net_balance_zar,
        total_deposit_balance_zar,
        total_loan_balance_zar,
        total_loan_exposure_zar,
        total_credit_limit_zar,
        total_card_balance_zar,
        total_ecl_zar,
        total_arrears_zar,
        npl_loan_count,

        -- Profitability
        total_nim_zar,
        total_fee_income_zar,
        total_net_profit_zar,
        total_cost_to_serve_zar,

        -- Engagement
        digital_engagement_pct,
        total_txn_count_12m,
        outgoing_payments_12m,
        avg_payment_failure_rate_pct,

        -- Risk flags
        confirmed_fraud_count,
        total_fraud_loss_zar,
        has_open_fraud_alert,

        current_timestamp()                                         as loaded_at
    from enriched
)

select * from final
