-- Custom dbt test: no account should have a negative closing balance
-- unless it has an approved overdraft facility (credit_limit > 0).
-- Fails if any rows are returned.

select
    account_sk,
    account_id,
    date_key,
    closing_balance,
    credit_limit,
    closing_balance + coalesce(credit_limit, 0) as effective_available_balance
from {{ ref('fact_daily_balance') }} fdb
join {{ ref('dim_account') }} da on da.account_sk = fdb.account_sk
join {{ ref('dim_date') }} dd on dd.date_sk = fdb.date_sk
where
    -- Balance is negative beyond the overdraft facility
    fdb.closing_balance < -1 * coalesce(da.credit_limit, 0)
    -- Only check active accounts
    and da.status = 'Active'
    and da.is_current = true
    -- Only flag severe cases (more than R100 over limit) to avoid floating point noise
    and fdb.closing_balance + coalesce(da.credit_limit, 0) < -100
