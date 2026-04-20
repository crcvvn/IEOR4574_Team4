with item_views as (
    select * from {{ ref('base_item_views') }}
),

item_summary as (
    select
        item_name,
        avg(price_per_unit)                as avg_price,
        sum(add_to_cart_quantity)          as total_added_to_cart,
        sum(remove_from_cart_quantity)     as total_removed_from_cart,
        count(distinct item_view_id)       as total_views,
        count(distinct session_id)         as sessions_with_views
    from item_views
    group by item_name
)

select
    item_name                                       as item_id,
    item_name,
    avg_price,
    total_views,
    sessions_with_views,
    total_added_to_cart,
    total_removed_from_cart,
    round(
        total_added_to_cart / nullif(total_views, 0), 4
    )                                               as add_to_cart_rate
from item_summary