select
    ORDER_ID as order_id,
    SESSION_ID as session_id,
    CLIENT_NAME as client_name,
    ORDER_AT as order_at,
    PAYMENT_METHOD as payment_method,
    PAYMENT_INFO as payment_info,
    PHONE as phone,
    SHIPPING_ADDRESS as shipping_address,
    try_to_number(replace(SHIPPING_COST, 'USD ', '')) as shipping_cost,
    STATE as state,
    TAX_RATE as tax_rate,
    "_fivetran_id" as fivetran_order_id,
    "_fivetran_deleted" as is_deleted,
    "_fivetran_synced" as fivetran_synced_at
from {{ source('web_data', 'orders') }}