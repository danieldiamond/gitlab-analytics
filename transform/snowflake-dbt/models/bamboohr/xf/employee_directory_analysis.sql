{{ config({
    "materialized":"table",
    "schema": "analytics"
    })
}}

WITH employee_directory AS (

    SELECT
    {{ dbt_utils.star(from=ref('employee_directory'), except=['LAST_JOB_TITLE', 'LAST_SUPERVISOR', 'LAST_DEPARTMENT', 'LAST_DIVISION', 'COST_CENTER', 'LOCATION_FACTOR']) }}
    FROM {{ ref('employee_directory') }}

), date_details AS (

    SELECT * FROM {{ ref('date_details') }}

), department_info AS (

    SELECT employee_id,
          job_title,
          effective_date,
          department,
          division,
          effective_end_date
    FROM {{ ref('bamboohr_job_info') }}


), location_factor AS (

  SELECT *
  FROM {{ ref('employee_location_factor_snapshots') }}

), joined AS (

    SELECT  date_actual,
            employee_directory.*,
            department_info.job_title,
            department_info.department,
            department_info.division,
            location_factor.location_factor,
            CASE WHEN hire_date = date_actual THEN True ELSE False END AS is_hire_date,
            CASE WHEN termination_date = dateadd('day', 1, date_actual) THEN True ELSE False END AS is_termination_date
    FROM date_details
    LEFT JOIN employee_directory
      ON hire_date::date <= date_actual
      AND COALESCE(termination_date::date, CURRENT_DATE) > date_actual
    LEFT JOIN department_info
      ON employee_directory.employee_id = department_info.employee_id
      AND effective_date <= date_actual
      AND COALESCE(effective_end_date::date, CURRENT_DATE) > date_actual
    LEFT JOIN location_factor
      ON employee_directory.employee_number::varchar = location_factor.bamboo_employee_number::varchar
      AND valid_from <= date_actual
      AND COALESCE(valid_to::date, CURRENT_DATE) > date_actual
    WHERE employee_directory.employee_id IS NOT NULL

)

SELECT date_actual,
        (first_name ||' '|| last_name) AS full_name,
        job_title,--the below case when statement is also used in bamboohr_job_info; 
        CASE WHEN division = 'Alliances' THEN 'Alliances'
             WHEN division = 'Customer Support' THEN 'Customer Support'
             WHEN division = 'Customer Service' THEN 'Customer Success'
             WHEN department = 'Data & Analytics' THEN 'Business Operations'
            ELSE nullif(department, '') END AS department,
        CASE WHEN department = 'Meltano' THEN 'Meltano'
             WHEN division = 'Employee' THEN null
             WHEN division = 'Contractor ' THEN null
             WHEN division = 'Alliances' Then 'Sales'
             WHEN division = 'Customer Support' THEN 'Engineering'
             WHEN division = 'Customer Service' THEN 'Sales'
          ELSE nullif(division, '') END AS division,
        COALESCE (location_factor, hire_location_factor) as location_factor,
        is_hire_date,
        is_termination_date
FROM joined
ORDER BY date_actual
--
--AND termination_date IS NULL
