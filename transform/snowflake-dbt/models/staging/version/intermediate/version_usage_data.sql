{{ config({
    "materialized": "view"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ ref('version_usage_data_source') }}
    WHERE uuid IS NOT NULL

)

SELECT *
FROM source
