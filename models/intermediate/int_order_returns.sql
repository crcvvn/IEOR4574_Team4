WITH deduplicated_orders AS(    
    SELECT *, 
    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_at ASC) AS row_num
    FROM {{ref('base_orders')}}
    WHERE is_deleted = FALSE
),

orders_table AS(
    SELECT order_id, session_id, client_name, order_at, payment_method, shipping_cost, state
    FROM deduplicated_orders
    WHERE row_num = 1
),

deduplicated_returns AS (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY order_id ORDER BY returned_at ASC) as row_num
    FROM {{ref('base_returns')}}
),

returns_table AS (
    SELECT order_id, returned_at, is_refunded
    FROM deduplicated_returns
    WHERE row_num = 1
)

SELECT o.order_id, o.session_id, o.client_name, o.order_at, o.payment_method, o.shipping_cost, o.state,
r.returned_at, r.is_refunded, r.order_id IS NOT NULL AS is_returned
FROM orders_table as o
LEFT JOIN returns_table as r
ON o.order_id = r.order_id