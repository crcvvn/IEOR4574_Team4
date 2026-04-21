with client_orders as (
    select * from {{ ref('int_client_orders') }}
)

select
    client_name                                     as client_id,
    client_name,
    total_orders,
    first_order_at,
    last_order_at,
    avg_shipping_cost,
    distinct_payment_methods,
    case
        when total_orders >= 10 then 'high_value'
        when total_orders >= 3  then 'returning'
        else 'one_time'
    end                                             as client_segment
from client_orders