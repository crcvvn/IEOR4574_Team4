WITH deduplicated_sessions AS(    
    SELECT *, 
    ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY session_at ASC) AS row_num
    FROM {{ref('base_sessions')}}
    WHERE is_deleted = FALSE
),

sessions_table AS (
    SELECT session_id, client_id, session_at, operating_system
    FROM deduplicated_sessions
    WHERE row_num = 1
),

page_views AS (
    SELECT session_id, 
    COUNT(DISTINCT page_view_id) AS total_page_views
    FROM {{ref('base_page_views')}}
    WHERE is_deleted = FALSE
    GROUP BY session_id
),

item_views AS (
    SELECT session_id, 
    COUNT(DISTINCT item_view_id) AS total_item_views,
    SUM(add_to_cart_quantity - remove_from_cart_quantity) as total_net_quantity_in_cart,
    COUNT(DISTINCT CASE WHEN add_to_cart_quantity - remove_from_cart_quantity > 0 THEN item_view_id END) AS distinct_items_in_cart
    FROM {{ref('base_item_views')}}
    WHERE is_deleted = FALSE
    GROUP BY session_id
)

SELECT s.session_id, s.client_id, s.session_at, s.operating_system,
COALESCE(pv.total_page_views, 0)    AS total_page_views,
COALESCE(iv.total_item_views, 0)    AS total_item_views,
COALESCE(iv.total_net_quantity_in_cart, 0)   AS total_net_quantity_in_cart,
COALESCE(iv.distinct_items_in_cart, 0) AS distinct_items_in_cart
FROM sessions_table AS s
LEFT JOIN page_views AS pv ON s.session_id = pv.session_id
LEFT JOIN item_views AS iv ON s.session_id = iv.session_id

