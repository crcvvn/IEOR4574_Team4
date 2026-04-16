select
    "_fivetran_id" as session_row_id,
    SESSION_ID as session_id,
    CLIENT_ID as client_id,
    try_to_timestamp_ntz(SESSION_AT) as session_at,
    OS as operating_system,
    IP as ip_address,
    "_fivetran_deleted" as is_deleted,
    "_fivetran_synced" as fivetran_synced_at
from {{ source('web_data', 'sessions') }}