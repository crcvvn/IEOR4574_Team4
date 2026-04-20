with orders as (
    select * from {{ ref('base_orders') }}
),

client_orders as (
    select
        client_name,
        count(distinct order_id)       as total_orders,
        min(order_at)                  as first_order_at,
        max(order_at)                  as last_order_at,
        avg(shipping_cost)             as avg_shipping_cost,
        count(distinct payment_method) as distinct_payment_methods
    from orders
    group by client_name
)

select * from client_orders