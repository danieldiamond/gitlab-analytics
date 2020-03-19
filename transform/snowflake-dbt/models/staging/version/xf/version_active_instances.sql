WITH version_checks AS (

    SELECT 
      host_id::VARCHAR        AS host_id,
      gitlab_version          AS version,
      created_at,
      'version_ping'          AS ping_type
    FROM {{ ref("version_version_checks_source") }}

), usage_data AS (

    SELECT
      COALESCE(to_varchar(host_id), to_varchar(uuid)) AS host_id,
      version,
      created_at,
      'usage_ping'                                    AS ping_type
    FROM {{ ref("version_usage_data") }}

), active_instances AS (

    SELECT * FROM version_checks
    UNION ALL
    SELECT * FROM usage_data

), active_instances_w_first_ping AS (

    SELECT *,
    MIN(date_trunc('day', created_at)) OVER (PARTITION BY host_id ORDER BY created_at) AS first_ping_at_date
    FROM active_instances

)

SELECT  
  host_id,
  version,
  DATE_TRUNC('day', created_at)                     AS created_at_date,
  first_ping_at_date,
  ping_type,
  datediff(month, first_ping_at_date, created_at)   AS months_since_first_ping
FROM active_instances_w_first_ping
{{ dbt_utils.group_by(n=6) }}
