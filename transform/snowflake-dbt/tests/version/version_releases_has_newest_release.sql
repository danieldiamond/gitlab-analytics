WITH max_usage_ping AS (

  SELECT MAX(created_at) AS max_timestamp
  FROM {{ ref('version_usage_data_unpacked') }}

),

max_release_date AS (

  SELECT MAX(release_date) AS max_timestamp
  FROM {{ ref('version_releases') }} 

)

/* Version releases should always have a higher max timestamp */
SELECT *
FROM max_usage_ping, max_release_date
WHERE max_usage_ping.max_timestamp >= max_release_date.max_timestamp
