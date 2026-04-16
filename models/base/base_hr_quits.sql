select
    employee_id,
    quit_date,
    _file as source_file,
    _line as source_line,
    cast(_modified as timestamp) as modified_at,
    cast(_fivetran_synced as timestamp) as fivetran_synced_at
from {{ source('google_drive', 'hr_quits') }}
where employee_id is not null