select
    "_fivetran_id" as page_view_id,
    PAGE_NAME as page_name,
    SESSION_ID as session_id,
    VIEW_AT as viewed_at,
    "_fivetran_deleted" as is_deleted,
    "_fivetran_synced" as fivetran_synced_at
from {{ source('web_data', 'page_views') }}