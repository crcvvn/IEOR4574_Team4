select
    employee_id,
    employee_name,
    city,
    hire_date,
    quit_date,
    is_active,
    tenure_days
from {{ ref('int_employee_status') }}