WITH source AS (

  SELECT *
  FROM {{ source('sheetload', 'hire_plan') }}

), renamed AS (

  SELECT 
    function::VARCHAR     AS function,
    department::VARCHAR   AS department,
    month_year::DATE      AS month_year,
    plan::NUMBER          AS plan
  FROM source

)

SELECT *
FROM renamed
