select
    cast(order_at as date) as finance_date,
    count(distinct order_id) as order_count,
    sum(coalesce(shipping_cost, 0)) as gross_revenue
from {{ ref('base_orders') }}
where order_id is not null
group by 1