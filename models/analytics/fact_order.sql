SELECT
    order_id,
    session_id,
    client_name,
    order_at,
    payment_method,
    shipping_cost,
    state,
    is_returned,
    COALESCE(is_refunded, FALSE)    AS is_refunded,
    returned_at
FROM {{ ref('int_order_returns') }}