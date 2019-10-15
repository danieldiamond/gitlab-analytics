WITH source as (

  SELECT *
  FROM {{ref('employee_directory_analysis')}}

)

SELECT *
FROM source
WHERE date_actual = CURRENT_date
  AND work_email != 't2test@gitlab.com'
  AND (department IS NULL OR division IS NULL)
