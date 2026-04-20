select
    j.employee_id,
    j.employee_name,
    j.city,
    j.hire_date,
    q.quit_date,
    case
        when q.quit_date is null then true
        else false
    end as is_active,
    datediff(
        day,
        j.hire_date,
        coalesce(q.quit_date, current_date)
    ) as tenure_days
from {{ ref('base_hr_joins') }} j
left join {{ ref('base_hr_quits') }} q
    on j.employee_id = q.employee_id