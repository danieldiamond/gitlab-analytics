{{ config({
    "materialized": "incremental",
    "schema": "analytics_sensitive"
    })
}}

{% set aggregated_column_names = "employee_id, job_title, department, division, reports_to, location_factor" %}

WITH employees AS (
  SELECT * 
  FROM "ANALYTICS"."ANALYTICS_SENSITIVE"."EMPLOYEE_LOCATION_FACTOR_SNAPSHOTS"

), employee_mapping AS (

  SELECT *
  FROM "ANALYTICS"."ANALYTICS_SENSITIVE"."BAMBOOHR_ID_EMPLOYEE_NUMBER_MAPPING"

), job_info AS (

  SELECT * 
  FROM "ANALYTICS"."ANALYTICS_SENSITIVE"."BAMBOOHR_JOB_INFO"  
  
), date_details AS(

    SELECT * 
    FROM analytics.analytics.date_details
    WHERE date_actual >= '2019-08-31'

), historical_location_factor as (

SELECT 
    date_details.date_actual,
    job_info.effective_date,
    job_info.effective_end_date,
    employee_mapping.first_name,
    employee_mapping.last_name,
    job_info.employee_id,
    job_title,
    department,
    division,
    reports_to,
    location_factor,
    valid_from
    
FROM date_details
LEFT JOIN job_info
  ON date_details.date_actual between job_info.effective_date and coalesce(job_info.effective_end_date, current_date())
LEFT JOIN employee_mapping 
  ON job_info.employee_id = employee_mapping.employee_id 
LEFT JOIN employees 
 ON employees.bamboo_employee_number = employee_mapping.employee_number
 AND date_details.date_actual between employees.valid_from and employees.valid_to

)

select 
    historical_location_factor.*
from historical_location_factor
WHERE QUALIFY ROW_NUMBER() OVER (PARTITION BY aggregated_column_names ORDER BY date_actual DESC) = 1


