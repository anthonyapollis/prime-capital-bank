{{
    config(
        materialized = 'table',
        tags         = ['mart', 'branch', 'operations', 'daily'],
        description  = 'Branch KPIs: new accounts, loans disbursed, deposits gathered, revenue, cost, staff productivity.'
    )
}}

/*
  mart_branch_performance
  ─────────────────────────────────────────────────────────────────────────────
  Sources : stg_accounts, stg_loans, stg_transactions, int_account_profitability
  Layer   : Gold (mart)
  Grain   : One row per branch (branch_id) × reporting_month
  Purpose : Branch-level operational and financial KPIs for the Branch
            Management dashboard, regional performance reviews, and
            staff incentive calculations.
  ─────────────────────────────────────────────────────────────────────────────
*/

with

-- Reporting month spine
reporting_month as (
    select date_trunc('month', current_date()) as rpt_month
),

-- ── New accounts opened this month ───────────────────────────────────────────
new_accounts as (
    select
        branch_id,
        date_trunc('month', account_open_date)                      as rpt_month,
        count(account_id)                                           as new_accounts_opened,
        count(case when account_type = 'CHEQUE'       then 1 end)   as new_cheque_accounts,
        count(case when account_type = 'SAVINGS'      then 1 end)   as new_savings_accounts,
        count(case when account_type = 'FIXED_DEPOSIT' then 1 end)  as new_fd_accounts,
        sum(current_balance)                                        as new_account_balance_zar
    from {{ ref('stg_accounts') }}
    where account_open_date >= date_trunc('month', date_add(current_date(), -12))
    group by branch_id, date_trunc('month', account_open_date)
),

-- ── Deposits gathered (average balance by branch, current month) ──────────────
deposits as (
    select
        branch_id,
        count(account_id)                                           as total_deposit_accounts,
        sum(current_balance)                                        as total_deposit_balance_zar,
        avg(current_balance)                                        as avg_deposit_balance_zar,
        count(case when is_dormant = true then 1 end)               as dormant_accounts_count
    from {{ ref('stg_accounts') }}
    where account_type in ('CHEQUE', 'SAVINGS', 'MONEY_MARKET', 'FIXED_DEPOSIT', 'NOTICE')
      and account_status_derived = 'ACTIVE'
    group by branch_id
),

-- ── Loans originated this month ───────────────────────────────────────────────
loan_originations as (
    select
        branch_id,
        date_trunc('month', origination_date)                       as rpt_month,
        count(loan_id)                                              as loans_disbursed,
        sum(original_loan_amount_zar)                               as loans_disbursed_value_zar,
        count(case when loan_type = 'HOME_LOAN'    then 1 end)      as home_loans_originated,
        count(case when loan_type = 'VEHICLE'      then 1 end)      as vehicle_loans_originated,
        count(case when loan_type = 'PERSONAL'     then 1 end)      as personal_loans_originated,
        count(case when loan_type = 'BUSINESS'     then 1 end)      as business_loans_originated,
        avg(original_loan_amount_zar)                               as avg_loan_size_zar,
        avg(interest_rate_pct)                                      as avg_loan_rate_pct
    from {{ ref('stg_loans') }}
    where origination_date >= date_trunc('month', date_add(current_date(), -12))
    group by branch_id, date_trunc('month', origination_date)
),

-- ── Active loan portfolio by branch ───────────────────────────────────────────
loan_portfolio as (
    select
        branch_id,
        count(loan_id)                                              as active_loan_count,
        sum(outstanding_principal_zar)                              as loan_book_balance_zar,
        count(case when npl_flag = true then 1 end)                 as npl_count,
        sum(case when npl_flag = true then outstanding_principal_zar else 0 end)
                                                                    as npl_balance_zar,
        sum(provision_amount_zar)                                   as total_provisions_zar,
        avg(delinquency_days)                                       as avg_delinquency_days
    from {{ ref('stg_loans') }}
    where loan_status = 'ACTIVE'
    group by branch_id
),

-- ── Branch transaction volumes ─────────────────────────────────────────────────
branch_transactions as (
    select
        branch_id,
        date_trunc('month', transaction_date)                       as rpt_month,
        count(transaction_id)                                       as total_transactions,
        count(case when channel_code = 'BRANCH' then 1 end)        as teller_transactions,
        sum(transaction_amount_zar)                                 as total_transaction_value_zar,
        sum(transaction_fee_zar)                                    as total_fee_income_zar,
        count(distinct customer_id)                                 as unique_customers_served
    from {{ ref('stg_transactions') }}
    where transaction_date >= date_trunc('month', date_add(current_date(), -12))
      and branch_id is not null
    group by branch_id, date_trunc('month', transaction_date)
),

-- ── Branch profitability (from account profitability model) ────────────────────
branch_profit as (
    select
        a.branch_id,
        sum(ap.nim_zar)                                             as total_nim_zar,
        sum(ap.total_fee_income_zar)                                as total_fee_income_zar,
        sum(ap.net_profit_zar)                                      as total_net_profit_zar,
        sum(ap.cost_to_serve_zar)                                   as total_cost_to_serve_zar,
        count(ap.account_id)                                        as managed_accounts
    from {{ ref('int_account_profitability') }} ap
    inner join {{ ref('stg_accounts') }} a using (account_id)
    group by a.branch_id
),

-- ── Join everything onto reporting_month × branch spine ───────────────────────
branch_spine as (
    select distinct branch_id
    from {{ ref('stg_accounts') }}
    where branch_id is not null
),

current_month_joined as (
    select
        bs.branch_id,
        rm.rpt_month                                                as reporting_month,

        -- New accounts
        coalesce(na.new_accounts_opened,    0)                      as new_accounts_opened,
        coalesce(na.new_cheque_accounts,    0)                      as new_cheque_accounts,
        coalesce(na.new_savings_accounts,   0)                      as new_savings_accounts,
        coalesce(na.new_fd_accounts,        0)                      as new_fd_accounts,
        coalesce(na.new_account_balance_zar, 0)                     as new_account_balance_zar,

        -- Deposits
        coalesce(d.total_deposit_accounts,  0)                      as total_deposit_accounts,
        coalesce(d.total_deposit_balance_zar, 0)                    as total_deposit_balance_zar,
        coalesce(d.dormant_accounts_count,  0)                      as dormant_accounts,

        -- Loans (originated this month)
        coalesce(lo.loans_disbursed,        0)                      as loans_disbursed,
        coalesce(lo.loans_disbursed_value_zar, 0)                   as loans_disbursed_value_zar,
        coalesce(lo.home_loans_originated,  0)                      as home_loans_originated,
        coalesce(lo.personal_loans_originated, 0)                   as personal_loans_originated,
        coalesce(lo.avg_loan_size_zar,      0)                      as avg_loan_size_zar,

        -- Loan portfolio (active)
        coalesce(lp.active_loan_count,      0)                      as active_loan_count,
        coalesce(lp.loan_book_balance_zar,  0)                      as loan_book_balance_zar,
        coalesce(lp.npl_count,              0)                      as npl_count,
        coalesce(lp.npl_balance_zar,        0)                      as npl_balance_zar,
        coalesce(lp.total_provisions_zar,   0)                      as total_provisions_zar,

        -- Transactions
        coalesce(bt.total_transactions,     0)                      as total_transactions,
        coalesce(bt.teller_transactions,    0)                      as teller_transactions,
        coalesce(bt.total_transaction_value_zar, 0)                 as total_transaction_value_zar,
        coalesce(bt.unique_customers_served, 0)                     as unique_customers_served,

        -- Revenue & profit
        coalesce(bp.total_nim_zar,          0)                      as total_nim_zar,
        coalesce(bp.total_fee_income_zar,   0)                      as total_fee_income_zar,
        coalesce(bp.total_net_profit_zar,   0)                      as total_net_profit_zar,
        coalesce(bp.total_cost_to_serve_zar, 0)                     as total_cost_to_serve_zar,
        coalesce(bp.managed_accounts,       0)                      as managed_accounts

    from branch_spine bs
    cross join reporting_month rm
    left join new_accounts na
        on bs.branch_id = na.branch_id
       and rm.rpt_month = na.rpt_month
    left join deposits d
        on bs.branch_id = d.branch_id
    left join loan_originations lo
        on bs.branch_id = lo.branch_id
       and rm.rpt_month = lo.rpt_month
    left join loan_portfolio lp
        on bs.branch_id = lp.branch_id
    left join branch_transactions bt
        on bs.branch_id = bt.branch_id
       and rm.rpt_month = bt.rpt_month
    left join branch_profit bp
        on bs.branch_id = bp.branch_id
),

-- ── Derived KPIs ──────────────────────────────────────────────────────────────
kpis as (
    select
        *,

        -- NPL rate
        case
            when loan_book_balance_zar > 0
            then round(npl_balance_zar / loan_book_balance_zar * 100, 2)
            else 0
        end                                                         as npl_rate_pct,

        -- Cost-to-income ratio
        case
            when (total_nim_zar + total_fee_income_zar) > 0
            then round(total_cost_to_serve_zar / (total_nim_zar + total_fee_income_zar) * 100, 2)
            else null
        end                                                         as cost_to_income_pct,

        -- Average revenue per account
        case
            when managed_accounts > 0
            then round((total_nim_zar + total_fee_income_zar) / managed_accounts, 2)
            else 0
        end                                                         as revenue_per_account_zar,

        -- Teller productivity (transactions per teller; placeholder teller count from txn volume)
        -- In production this joins to HR feed for headcount
        case
            when teller_transactions > 0
            then round(teller_transactions / greatest(managed_accounts / 500.0, 1), 1)
            else 0
        end                                                         as txn_per_teller_proxy,

        -- Branch ranking by net profit (window)
        rank() over (order by total_net_profit_zar desc)            as profit_rank,
        rank() over (order by new_accounts_opened desc)             as growth_rank,
        rank() over (order by npl_balance_zar asc)                  as asset_quality_rank

    from current_month_joined
),

final as (
    select
        reporting_month,
        branch_id,
        -- Accounts
        new_accounts_opened,
        new_cheque_accounts,
        new_savings_accounts,
        new_fd_accounts,
        new_account_balance_zar,
        total_deposit_accounts,
        total_deposit_balance_zar,
        dormant_accounts,
        managed_accounts,
        -- Loans
        loans_disbursed,
        loans_disbursed_value_zar,
        home_loans_originated,
        personal_loans_originated,
        avg_loan_size_zar,
        active_loan_count,
        loan_book_balance_zar,
        npl_count,
        npl_balance_zar,
        npl_rate_pct,
        total_provisions_zar,
        -- Transactions
        total_transactions,
        teller_transactions,
        total_transaction_value_zar,
        unique_customers_served,
        txn_per_teller_proxy,
        -- Revenue
        total_nim_zar,
        total_fee_income_zar,
        total_net_profit_zar,
        total_cost_to_serve_zar,
        cost_to_income_pct,
        revenue_per_account_zar,
        -- Rankings
        profit_rank,
        growth_rank,
        asset_quality_rank,
        current_timestamp()                                         as loaded_at
    from kpis
)

select * from final
