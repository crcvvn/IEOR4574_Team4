select
    cast(expense_date as date) as finance_date,
    sum(coalesce(expense_amount, 0)) as total_cost
from {{ ref('base_expenses') }}
group by 1