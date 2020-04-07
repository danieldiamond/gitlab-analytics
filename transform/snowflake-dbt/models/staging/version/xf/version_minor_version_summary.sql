WITH source AS (

  SELECT *
  FROM {{ ref('version_usage_data') }}

),

aggregated AS (

  SELECT
    major_version,
    minor_version,
    MIN(created_at),
    MAX(created_at)
  FROM source
  GROUP BY 1,2

)

SELECT *
FROM source
