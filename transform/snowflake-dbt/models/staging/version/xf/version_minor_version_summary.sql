WITH usage_data AS (

  SELECT *
  FROM {{ ref('version_usage_data') }}

),

release_schedule AS (

  SELECT *
  FROM {{ ref('gitlab_release_schedule') }}

),

aggregated AS (

  SELECT
    major_version,
    minor_version,
    MIN(created_at),
    MAX(created_at)
  FROM usage_data
  GROUP BY 1,2

)

SELECT *
FROM source
