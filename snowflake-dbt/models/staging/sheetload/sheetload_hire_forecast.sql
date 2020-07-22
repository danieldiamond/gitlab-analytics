WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_hire_forecast_source') }}

)

SELECT *
FROM source