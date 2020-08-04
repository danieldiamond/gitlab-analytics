WITH source AS (

  SELECT *
  FROM {{ source('sheetload', 'hire_forecast') }}

), renamed AS (

  SELECT 
    function::VARCHAR       AS function,
    department::VARCHAR     AS department,
    month_year::DATE        AS month_year,
    TRY_TO_NUMBER(forecast) AS forecast
  FROM source

)

SELECT *
FROM renamed
WHERE forecast IS NOT NULL
