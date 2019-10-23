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
         nullif("Location_Factor",'')::float as location_factor,
         nullif(nullif("DEVIATION_FROM_COMP_CALC", ''), '#N/A')::varchar as deviation_from_comp_calc
    FROM source
    WHERE lower(bamboo_employee_number) NOT LIKE '%not in comp calc%'
)

SELECT bamboo_employee_number::bigint as bamboo_employee_number,
        location_factor,
        deviation_from_comp_calc
FROM renamed
