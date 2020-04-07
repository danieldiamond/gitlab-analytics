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
  
    usage_data.major_version,
    usage_data.minor_version,
    usage_data.major_minor_version,
    release_schedule.release_date,

    MIN(usage_data.created_at),
    MAX(usage_data.created_at)

  FROM usage_data
    LEFT JOIN release_schedule
      ON usage_data.major_minor_version = release_schedule.major_minor_version
  GROUP BY 1,2,3

)

SELECT *
FROM source
