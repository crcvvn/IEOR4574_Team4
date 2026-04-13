select
    order_id,
    returned_at,
    case 
        when lower(trim(is_refunded)) = 'yes' then true
        when lower(trim(is_refunded)) = 'no' then false
        else null
    end as is_refunded,
    _file as source_file,
    _line as source_line,
    cast(_modified as timestamp) as modified_at,
    cast(_fivetran_synced as timestamp) as fivetran_synced_at
from {{ source('google_drive', 'returns') }}
where order_id is not null