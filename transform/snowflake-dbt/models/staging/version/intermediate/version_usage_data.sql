{{ config({
    "materialized": "view"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ ref('version_usage_data_source') }}
  WHERE uuid IS NOT NULL
    AND CHECK_JSON(counts) IS NULL

)

SELECT *
FROM source
