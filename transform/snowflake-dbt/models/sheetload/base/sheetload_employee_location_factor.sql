{{ config(
    { "schema": "sensitive", 
      "materialized": "table" }
) }}

with source as (

    SELECT *
    FROM {{ source('sheetload', 'employee_location_factor') }}

), renamed as (

    SELECT
         nullif("Employee_ID",'') as bamboo_employee_id,
         nullif("Location_Factor",'')::float as location_factor,
         nullif(nullif("Metrics_Adjusted_Location_Factor", ''), 'ok')::float as metrics_adjusted_location_factor
    FROM source
    WHERE lower(bamboo_employee_id) NOT LIKE '%not in comp calc%'
)

SELECT bamboo_employee_id::bigint as bamboo_employee_id,
       coalesce(metrics_adjusted_location_factor, location_factor) as location_factor
FROM renamed
