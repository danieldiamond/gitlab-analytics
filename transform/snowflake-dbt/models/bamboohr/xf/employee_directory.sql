with bamboohr_directory as (

    SELECT *
    FROM {{ ref ('bamboohr_directory') }}

), hire_dates as (

    SELECT *
    FROM {{ ref ('bamboohr_employment_status') }}
    WHERE termination_type IS NULL

), termination_dates as (

    SELECT *
    FROM {{ ref ('bamboohr_employment_status') }}
    WHERE termination_type IS NOT NULL

), department_info as (

    SELECT employee_id,
            last_value(department) RESPECT NULLS OVER ( PARTITION BY employee_id ORDER BY effective_date ) AS department,
            last_value(division) RESPECT NULLS OVER ( PARTITION BY employee_id ORDER BY effective_date ) AS division
    FROM {{ ref ('bamboohr_job_info') }}

), mapping as (

    SELECT *
    FROM {{ref('bamboohr_id_employee_number_mapping')}}

), location_factor as (

    SELECT *
    FROM {{ref('sheetload_employee_location_factor')}}

), cost_center as (

    SELECT *
    FROM {{ref('cost_center_division_department_mapping')}}
)

SELECT  distinct mapping.employee_id,
        mapping.first_name,
        mapping.last_name,
        mapping.employee_number,
        bamboohr_directory.job_title,
        bamboohr_directory.supervisor,
        bamboohr_directory.work_email,
        hire_dates.effective_date as hire_date,
        termination_dates.effective_date as termination_date,
        department_info.department,
        department_info.division,
        cost_center.cost_center,
        location_factor.location_factor,
        convert_timezone('UTC',current_timestamp()) AS _last_dbt_run
FROM mapping
LEFT JOIN bamboohr_directory
  ON bamboohr_directory.employee_id = mapping.employee_id
LEFT JOIN hire_dates
  ON bamboohr_directory.employee_id::bigint = hire_dates.employee_id::bigint
LEFT JOIN termination_dates
  ON bamboohr_directory.employee_id::bigint = termination_dates.employee_id::bigint
  AND (termination_dates.effective_date > hire_dates.effective_date OR termination_dates.effective_date IS NULL)
LEFT JOIN department_info
  ON bamboohr_directory.employee_id::bigint = department_info.employee_id::bigint
LEFT JOIN cost_center
  ON department_info.department=cost_center.department
 AND department_info.division=cost_center.division
LEFT JOIN location_factor
  ON location_factor.bamboo_employee_number = mapping.employee_number
ORDER BY hire_date DESC
