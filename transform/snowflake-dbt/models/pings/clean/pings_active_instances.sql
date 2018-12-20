{{
  config(
    materialized='incremental',
    sql_where='TRUE',
    unique_key='unique_key'
  )
}}

WITH active_instances AS (
    SELECT
      to_varchar(host_id) as host_id,
      gitlab_version AS version,
      created_at,
      'version_ping' AS ping_type
    FROM {{ ref("pings_version_checks") }}
    {% if adapter.already_exists(this.schema, this.table) and not flags.FULL_REFRESH %}
        WHERE created_at >= DATE_TRUNC('month', CURRENT_DATE)
    {% endif %}

    UNION ALL
    SELECT
      coalesce(to_varchar(host_id), to_varchar(uuid)) as host_id,
      version,
      created_at,
      'usage_ping' AS ping_type
    FROM {{ ref("pings_usage_data") }}
    {% if adapter.already_exists(this.schema, this.table) and not flags.FULL_REFRESH %}
        WHERE created_at >= DATE_TRUNC('month', CURRENT_DATE)
    {% endif %}
)

SELECT
  md5(host_id || version || date_trunc('day', created_at)::date || ping_type) AS unique_key,
  host_id,
  version,
  DATE_TRUNC('day', created_at) AS created_at_date,
  ping_type
FROM active_instances
GROUP BY 1,2,3,4,5
