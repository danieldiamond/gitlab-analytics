{{ config({
    "materialized":"table",
    "schema": "analytics"
    })
}}

WITH employee_directory_intermediate AS (

   SELECT * FROM {{ref('employee_directory_intermediate')}}


), cleaned AS (

    SELECT 
      date_actual,
      employee_id,
      reports_to,
      (first_name ||' '|| last_name) AS full_name,
      work_email,
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
      COALESCE (location_factor, hire_location_factor) AS location_factor,
      is_hire_date,
      is_termination_date,
      hire_date,
      cost_center
    FROM employee_directory_intermediate

), final AS (

    SELECT
      {{ dbt_utils.surrogate_key('date_actual', 'employee_id') }} AS unique_key,
      cleaned.*
    FROM cleaned

)

SELECT DISTINCT *
FROM final
