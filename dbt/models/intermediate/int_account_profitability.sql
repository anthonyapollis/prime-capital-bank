{{
    config(
        materialized = 'table',
        tags         = ['intermediate', 'profitability', 'daily'],
        description  = 'Account-level profitability: NIM, fee income, cost-to-serve estimate, and net profit per account.'
    )
}}

/*
  int_account_profitability
  ─────────────────────────────────────────────────────────────────────────────
  Sources : stg_accounts, stg_transactions, stg_credit_cards
  Layer   : Intermediate
  Grain   : One row per account (account_id) for the current rolling 12 months
  Purpose : Estimate account-level profitability for the PCB Management
            Accounting model. Components:
              • Net Interest Margin (NIM) = interest earned − funding cost
              • Fee income = transaction fees + monthly fees
              • Cost-to-serve = channel cost estimate based on txn mix
              • Net profit per account
  ─────────────────────────────────────────────────────────────────────────────
*/

with

accounts as (
    select
        account_id,
        customer_id,
        branch_id,
        account_type,
        product_code,
        account_status_derived,
        current_balance,
        interest_rate_pct,
        monthly_fee_zar,
        account_age_days,
        is_dormant
    from {{ ref('stg_accounts') }}
),

-- ── Transaction aggregates (rolling 12 months) ────────────────────────────────
txn_agg as (
    select
        account_id,
        count(transaction_id)                                       as txn_count_12m,

        -- Volume by channel (for cost-to-serve)
        count(case when channel_code = 'BRANCH'           then 1 end) as branch_txn_count,
        count(case when channel_code = 'ATM'              then 1 end) as atm_txn_count,
        count(case when channel_code = 'INTERNET_BANKING' then 1 end) as ib_txn_count,
        count(case when channel_code = 'MOBILE_BANKING'   then 1 end) as mobile_txn_count,
        count(case when channel_code = 'POS'              then 1 end) as pos_txn_count,

        -- Gross turnover
        sum(case when is_credit then transaction_amount_zar else 0 end) as gross_credits_zar,
        sum(case when is_debit  then transaction_amount_zar else 0 end) as gross_debits_zar,

        -- Fee income collected from customer
        sum(coalesce(transaction_fee_zar, 0))                      as txn_fee_income_zar,

        -- Average daily balance proxy (average of recorded balances — full WADB needs snapshots)
        avg(transaction_amount_zar)                                 as avg_txn_amount_zar

    from {{ ref('stg_transactions') }}
    where transaction_date >= date_add(current_date(), -365)
      and transaction_status = 'COMPLETED'
    group by account_id
),

-- ── Credit card aggregates per account ────────────────────────────────────────
cc_agg as (
    select
        account_id,
        sum(current_balance_zar)                                    as cc_total_balance_zar,
        sum(total_purchases_mtd_zar * 12)                          as cc_annual_spend_zar,
        sum(annual_fee_zar)                                         as cc_annual_fee_zar,
        -- Revolving interest income estimate: balance × (monthly rate) × 12
        sum(
            current_balance_zar * (purchase_interest_rate_pct / 100.0)
        )                                                           as cc_interest_income_est_zar,
        count(card_id)                                              as active_cards
    from {{ ref('stg_credit_cards') }}
    where card_status = 'ACTIVE'
    group by account_id
),

-- ── Combine sources ────────────────────────────────────────────────────────────
combined as (
    select
        a.*,

        -- Transaction metrics
        coalesce(t.txn_count_12m,       0)                          as txn_count_12m,
        coalesce(t.branch_txn_count,    0)                          as branch_txn_count,
        coalesce(t.atm_txn_count,       0)                          as atm_txn_count,
        coalesce(t.ib_txn_count,        0)                          as ib_txn_count,
        coalesce(t.mobile_txn_count,    0)                          as mobile_txn_count,
        coalesce(t.pos_txn_count,       0)                          as pos_txn_count,
        coalesce(t.gross_credits_zar,   0)                          as gross_credits_zar,
        coalesce(t.gross_debits_zar,    0)                          as gross_debits_zar,
        coalesce(t.txn_fee_income_zar,  0)                          as txn_fee_income_zar,

        -- Credit card metrics
        coalesce(cc.cc_total_balance_zar,        0)                 as cc_total_balance_zar,
        coalesce(cc.cc_annual_spend_zar,         0)                 as cc_annual_spend_zar,
        coalesce(cc.cc_annual_fee_zar,           0)                 as cc_annual_fee_zar,
        coalesce(cc.cc_interest_income_est_zar,  0)                 as cc_interest_income_est_zar,
        coalesce(cc.active_cards,                0)                 as active_cards

    from accounts a
    left join txn_agg t  using (account_id)
    left join cc_agg  cc using (account_id)
),

-- ── NIM calculation ────────────────────────────────────────────────────────────
-- PCB funds transfer pricing (FTP) rate: internal cost of funding = repo + 75bps
-- Using simplified approach: asset yield − FTP cost
nim_calc as (
    select
        *,

        -- Average balance proxy (use current balance as approximation)
        current_balance                                             as avg_balance_zar,

        -- Interest income earned on deposit / lending products
        round(
            current_balance * (interest_rate_pct / 100.0)
            + cc_interest_income_est_zar
        , 2)                                                        as gross_interest_income_zar,

        -- Funding cost (FTP rate = PCB prime-linked internal rate, proxy 7.5%)
        round(current_balance * 0.075, 2)                           as funding_cost_zar,

        -- Monthly account fee (annualised)
        round(monthly_fee_zar * 12, 2)                              as annual_account_fee_zar

    from combined
),

cost_to_serve as (
    select
        *,

        -- ── Cost-to-serve: per-channel unit costs (PCB operational benchmarks) ─
        -- Branch: R65/txn, ATM: R12/txn, IB: R3/txn, Mobile: R2/txn, POS: R4/txn
        round(
            (branch_txn_count * 65.0)
            + (atm_txn_count    * 12.0)
            + (ib_txn_count     * 3.0)
            + (mobile_txn_count * 2.0)
            + (pos_txn_count    * 4.0)
            -- Fixed overhead allocation per active account
            + 480.0   -- R480/year per account
        , 2)                                                        as cost_to_serve_zar

    from nim_calc
),

profitability as (
    select
        *,

        -- ── Net Interest Margin per account ───────────────────────────────────
        round(gross_interest_income_zar - funding_cost_zar, 2)     as nim_zar,

        -- ── Total fee income ──────────────────────────────────────────────────
        round(
            txn_fee_income_zar + annual_account_fee_zar + cc_annual_fee_zar,
        2)                                                          as total_fee_income_zar,

        -- ── Net profit = NIM + fees − cost-to-serve ───────────────────────────
        round(
            (gross_interest_income_zar - funding_cost_zar)
            + txn_fee_income_zar + annual_account_fee_zar + cc_annual_fee_zar
            - cost_to_serve_zar
        , 2)                                                        as net_profit_zar

    from cost_to_serve
),

profitability_banded as (
    select
        *,

        -- ── Profitability band ────────────────────────────────────────────────
        case
            when net_profit_zar >= 10000   then 'Highly Profitable (>10K)'
            when net_profit_zar >= 2000    then 'Profitable (2K–10K)'
            when net_profit_zar >= 0       then 'Marginally Profitable (0–2K)'
            when net_profit_zar >= -2000   then 'Loss-Making (-2K–0)'
            else                                'Significantly Loss-Making (<-2K)'
        end                                                         as profitability_band,

        -- ── Digital engagement score (% txns via digital channels) ───────────
        case
            when txn_count_12m > 0
            then round(
                    (ib_txn_count + mobile_txn_count) / txn_count_12m * 100,
                 1)
            else 0
        end                                                         as digital_engagement_pct

    from profitability
),

final as (
    select
        account_id,
        customer_id,
        branch_id,
        account_type,
        product_code,
        account_status_derived,
        account_age_days,
        is_dormant,
        active_cards,
        txn_count_12m,
        branch_txn_count,
        atm_txn_count,
        ib_txn_count,
        mobile_txn_count,
        pos_txn_count,
        digital_engagement_pct,
        gross_credits_zar,
        gross_debits_zar,
        avg_balance_zar,
        interest_rate_pct,
        gross_interest_income_zar,
        funding_cost_zar,
        nim_zar,
        txn_fee_income_zar,
        annual_account_fee_zar,
        cc_annual_fee_zar,
        total_fee_income_zar,
        cc_total_balance_zar,
        cc_annual_spend_zar,
        cc_interest_income_est_zar,
        cost_to_serve_zar,
        net_profit_zar,
        profitability_band,
        current_timestamp()                                         as loaded_at
    from profitability_banded
)

select * from final
