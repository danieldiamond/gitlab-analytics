{{ config({
    "schema": "temporary"
    })
}}

with source as (

    SELECT *
    FROM {{ source('snapshots', 'sheetload_employee_location_factor_snapshots') }}
    WHERE "Employee_ID" != 'Not In Comp Calc'
      AND "Employee_ID" NOT IN ('$72,124','S1453')

)

SELECT *
FROM source
