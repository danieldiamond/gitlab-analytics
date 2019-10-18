{{ config({
    "materialized":"table",
    "schema": "sensitive"
    })
}}

{% set max_date_in_analysis = "date_trunc('week', dateadd(week, 3, CURRENT_DATE))" %}

WITH employee_directory AS (

    SELECT
      employee_id,
      employee_number,
      first_name,
      last_name,
      work_email,
      hire_date,
      termination_date,
      hire_location_factor
    FROM {{ ref('employee_directory') }}

), date_details AS (

    SELECT * 
    FROM {{ ref('date_details') }}

), department_info AS (

    SELECT employee_id,
          job_title,
          effective_date,
          department,
          division,
          reports_to,
          effective_end_date
    FROM {{ ref('bamboohr_job_info') }}


), location_factor AS (

  SELECT *
  FROM {{ ref('employee_location_factor_snapshots') }}

), cost_center as (

    SELECT *
    FROM {{ref('cost_center_division_department_mapping')}}

)

SELECT  date_actual,
        employee_directory.*,
        department_info.job_title,
        department_info.department,
        department_info.division,
        department_info.reports_to,
        location_factor.location_factor,
        CASE WHEN hire_date = date_actual THEN True ELSE False END AS is_hire_date,
        CASE WHEN termination_date = dateadd('day', 1, date_actual) THEN True ELSE False END AS is_termination_date
FROM date_details
LEFT JOIN employee_directory
  ON hire_date::date <= date_actual
  AND COALESCE(termination_date::date, {{max_date_in_analysis}}) > date_actual
LEFT JOIN department_info
  ON employee_directory.employee_id = department_info.employee_id
  AND effective_date <= date_actual
  AND COALESCE(effective_end_date::date, {{max_date_in_analysis}}) > date_actual
LEFT JOIN location_factor
  ON employee_directory.employee_number::varchar = location_factor.bamboo_employee_number::varchar
  AND valid_from <= date_actual
  AND COALESCE(valid_to::date, {{max_date_in_analysis}}) > date_actual
WHERE employee_directory.employee_id IS NOT NULL
