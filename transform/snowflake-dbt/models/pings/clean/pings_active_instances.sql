WITH active_instances AS (
    SELECT
      to_varchar(host_id) as host_id,
      gitlab_version AS version,
      created_at,
      'version_ping' AS ping_type
    FROM {{ ref("pings_version_checks") }}
    UNION ALL
    SELECT
      coalesce(to_varchar(host_id), to_varchar(uuid)) as host_id,
      version,
      created_at,
      'usage_ping' AS ping_type
    FROM {{ ref("pings_usage_data") }}
),
active_instances_w_first_ping AS (
 SELECT *,
  min(date_trunc('day', created_at)) OVER (PARTITION BY host_id ORDER BY created_at) as first_ping_at_date
 FROM active_instances
)

SELECT
  host_id,
  version,
  DATE_TRUNC('day', created_at) AS created_at_date,
  first_ping_at_date,
  ping_type,
  datediff(month, first_ping_at_date, created_at) as months_since_first_ping
FROM active_instances_w_first_ping
GROUP BY 1,2,3,4,5,6
