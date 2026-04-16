select
    employee_id,
    lower(trim(name)) as employee_name,
    lower(trim(city)) as city,
    lower(trim(address)) as address,
    lower(trim(title)) as job_title,
    annual_salary,
    try_to_date(replace(hire_date, 'day ', '')) as hire_date,
    _file as source_file,
    _line as source_line,
    cast(_modified as timestamp) as modified_at,
    cast(_fivetran_synced as timestamp) as fivetran_synced_at
from {{ source('google_drive', 'hr_joins') }}
where employee_id is not null