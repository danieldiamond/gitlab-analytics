{{ config({
    "schema": "temporary"
    })
}}

with source as (

    SELECT *
    FROM {{ source('snapshots', 'sheetload_kpi_status_snapshots') }}

)

SELECT *
FROM source
