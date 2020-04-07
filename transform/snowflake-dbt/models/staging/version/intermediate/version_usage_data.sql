{{ config({
    "materialized": "view"
    })
}}

WITH source AS (

    SELECT
      *,
      SPLIT_PART(version, '.', 1)::INT      AS major_version,
      SPLIT_PART(version, '.', 2)::INT      AS minor_version,
      major_version || '.' || minor_version AS major_minor_version
    FROM {{ ref('version_usage_data_source') }}
    WHERE uuid IS NOT NULL

)

SELECT *
FROM source
