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
         nullif("Location_Factor",'')::float as location_factor
    FROM source
    WHERE lower(bamboo_employee_id) NOT LIKE '%not in comp calc%'
)

SELECT bamboo_employee_id::bigint as bamboo_employee_id,
       location_factor
FROM renamed
