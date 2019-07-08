{{ config({
    "schema": "analytics",
    "post-hook": "grant select on {{this}} to role reporter"
    })
}}

with employee_directory as (

    SELECT *
    FROM {{ref('employee_directory')}}
    WHERE hire_date < CURRENT_DATE

), date_details as (

  SELECT * FROM {{ref('date_details')}}

), joined as (

  SELECT  date_actual,
          employee_directory.*
  FROM date_details
  LEFT JOIN employee_directory
  ON hire_date::date <= date_actual
  AND COALESCE(termination_date::date, CURRENT_DATE) > date_actual
  WHERE employee_id IS NOT NULL

)

SELECT date_actual,
        (first_name ||' '|| last_name) as full_name,
        job_title,
        department,
        division,
        cost_center
FROM joined
--
--AND termination_date IS NULL
