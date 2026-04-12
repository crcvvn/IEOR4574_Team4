select *
from {{ source('web_data', 'page_views') }}