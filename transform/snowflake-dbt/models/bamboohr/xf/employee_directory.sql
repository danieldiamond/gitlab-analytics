with bamboohr_directory as (

  SELECT *
  FROM {{ ref ('bamboohr_directory') }}

), hire_dates as (

    SELECT effective_date, employee_id
    FROM {{ ref ('bamboohr_compensation') }}
    WHERE compensation_change_reason = 'Hire'

), department_info as (

    SELECT employee_id,
            last_value(department) RESPECT NULLS OVER ( PARTITION BY employee_id ORDER BY effective_date ) as department,
            last_value(division) RESPECT NULLS OVER ( PARTITION BY employee_id ORDER BY effective_date ) as division
    FROM {{ ref ('bamboohr_job_info') }}
)
-- needs location factor info from sheetlaod

SELECT bamboohr_directory.full_name,
        bamboohr_directory.job_title,
        hire_dates.effective_date,
        department_info.department,
        department_info.division
FROM bamboohr_directory
LEFT JOIN hire_dates
  ON bamboohr_directory.employee_id = hire_dates.employee_id
LEFT JOIN department_info
  ON bamboohr_directory.employee_id = department_info.employee_id
GROUP BY 1, 2, 3, 4, 5
ORDER BY effective_date DESC
