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
    
      release_schedule.major_minor_version,
      release_schedule.release_date,
      usage_data.major_version,
      usage_data.minor_version,

      ROW_NUMBER() OVER (ORDER BY release_schedule.major_minor_version)           AS version_row_number,
      LAG(release_schedule.release_date) OVER (
        ORDER BY release_schedule.major_minor_version
      )                                                                           AS next_version_release_date,

      MIN(IFF(NOT usage_data.version_is_prerelease, usage_data.created_at, NULL)) AS min_usage_ping_created_at,
      MAX(IFF(NOT usage_data.version_is_prerelease, usage_data.created_at, NULL)) AS max_usage_ping_created_at

    FROM release_schedule
      LEFT JOIN usage_data
        ON usage_data.major_minor_version = release_schedule.major_minor_version
    GROUP BY 1,2,3,4
    ORDER BY 
      1 DESC,
      2

)

SELECT *
FROM aggregated
