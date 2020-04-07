{{ config({
    "materialized": "view"
    })
}}

WITH source AS (

    SELECT
      *,
      SPLIT_PART(version, '.', 1)::INT AS major_version,
      SPLIT_PART(version, '.', 2)::INT AS minor_version
    FROM {{ ref('version_usage_data_source') }}
    WHERE uuid IS NOT NULL

    LIMIT 1000000 -- TEMP

)

SELECT *
FROM source
