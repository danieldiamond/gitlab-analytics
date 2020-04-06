{{ config({
    "materialized": "view"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ ref('version_usage_data_source') }}
    WHERE uuid IS NOT NULL
      AND hostname NOT IN ( -- Staging data has no current use cases for analysis.
        'staging.gitlab.com'
        'dr.gitlab.com'
      )

)

SELECT *
FROM source
