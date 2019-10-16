WITH source as (

  SELECT *
  FROM {{ref('employee_directory_analysis')}}

)

SELECT *
FROM source
WHERE date_actual = CURRENT_date
  AND (department IS NULL OR division IS NULL)
