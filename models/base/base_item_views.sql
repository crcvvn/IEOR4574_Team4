select *
from {{ source('web_data', 'item_views') }}