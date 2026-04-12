select *
from {{ source('web_data', 'sessions') }}