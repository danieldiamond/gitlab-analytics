WITH max_usage_ping AS (

  SELECT MAX(created_at) AS max_usage_ping
  FROM {{ ref('version_usage_data_unpacked') }}

),

max_release_date AS (

  SELECT MAX(release_date) AS max_release_date
  FROM {{ ref('version_releases') }} 

)

/* Version releases should always have a higher max timestamp */
SELECT *
FROM max_usage_ping
  INNER JOIN max_release_date
    ON max_usage_ping.max_usage_ping >= max_release_date.max_release_date