WITH source as (

  SELECT *
  FROM {{ref('employee_directory')}}

)

SELECT *
FROM source
WHERE hire_location_factor IS NULL
AND termination_date IS NULL
AND CURRENT_DATE > dateadd('days', 9, hire_date)
