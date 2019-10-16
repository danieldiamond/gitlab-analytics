{{ config({
    "materialized":"table",
    "schema": "analytics"
    })
}}

{% set max_date_in_analysis = "date_trunc('week', dateadd(week, 3, CURRENT_DATE))" %}

WITH employee_directory_intermediate AS (

   SELECT * FROM {{ref('employee_directory_intermediate')}}


), cleaned as (

    SELECT date_actual,
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
            COALESCE (location_factor, hire_location_factor) as location_factor,
            is_hire_date,
            is_termination_date,
            hire_date
    FROM employee_directory_intermediate

), final as (

    SELECT cleaned.*, cost_center.cost_center
    FROM cleaned
    LEFT JOIN cost_center
      ON cleaned.department=cost_center.department
     AND cleaned.division=cost_center.division

)

SELECT distinct *
FROM final
