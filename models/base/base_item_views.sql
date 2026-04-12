select
    "_fivetran_id" as item_view_id,
    SESSION_ID as session_id,
    ITEM_NAME as item_name,
    ADD_TO_CART_QUANTITY as add_to_cart_quantity,
    REMOVE_FROM_CART_QUANTITY as remove_from_cart_quantity,
    PRICE_PER_UNIT as price_per_unit,
    ITEM_VIEW_AT as item_view_at,
    "_fivetran_deleted" as is_deleted,
    "_fivetran_synced" as fivetran_synced_at
from {{ source('web_data', 'item_views') }}