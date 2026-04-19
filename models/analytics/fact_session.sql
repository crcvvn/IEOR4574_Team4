WITH session_orders AS (
    SELECT DISTINCT session_id
    FROM {{ ref('int_order_returns') }}
    WHERE is_returned = FALSE OR is_returned IS NULL
)

SELECT
    s.session_id,
    s.client_id,
    s.session_at,
    s.operating_system,
    s.total_page_views,
    s.total_item_views,
    s.distinct_items_in_cart,
    s.total_page_views > 0          AS has_page_view,
    s.total_item_views > 0          AS has_item_view,
    s.distinct_items_in_cart > 0    AS has_add_to_cart,
    o.session_id IS NOT NULL        AS has_order
FROM {{ ref('int_session_pageviews') }} AS s
LEFT JOIN session_orders AS o 
ON s.session_id = o.session_id