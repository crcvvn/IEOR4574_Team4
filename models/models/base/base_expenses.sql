select 
    date as expense_date,
    lower(trim(expense_type)) as expense_type,
    cast(replace(expense_amount, '$', '') as numeric(18,2)) as expense_amount,
    _file as source_file,
    _line as source_line,
    cast(_modified as timestamp) as modified_at,
    cast(_fivetran_synced as timestamp) as fivetran_synced_at
from {{ source('google_drive', 'expenses') }}
where expense_amount is not null
