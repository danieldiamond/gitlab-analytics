{{ config({
    "schema": "sensitive",
    "materialized": "table"
    })
}}

with source as (

    SELECT *
    FROM {{ source('sheetload', 'employee_location_factor') }}

), renamed as (

    SELECT
         nullif("Employee_ID",'')::varchar as bamboo_employee_number,
         nullif("Location_Factor",'')::float as location_factor
    FROM source 
    WHERE lower(bamboo_employee_number) NOT LIKE '%not in comp calc%'
)

SELECT bamboo_employee_number::bigint as bamboo_employee_number,
        location_factor,
        convert_timezone('America/Los_Angeles',convert_timezone('UTC',current_timestamp())) AS _last_dbt_run
FROM renamed
