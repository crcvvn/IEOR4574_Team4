select
    coalesce(r.finance_date, c.finance_date) as finance_date,
    coalesce(r.order_count, 0) as order_count,
    coalesce(r.gross_revenue, 0) as gross_revenue,
    coalesce(c.total_cost, 0) as total_cost,
    coalesce(r.gross_revenue, 0) - coalesce(c.total_cost, 0) as profit
from {{ ref('int_daily_revenue') }} r
full outer join {{ ref('int_daily_costs') }} c
    on r.finance_date = c.finance_date