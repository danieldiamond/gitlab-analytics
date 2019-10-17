-- depends_on: ref('version_usage_stats_to_stage_mappings')
{% set stage_list = dbt_utils.get_column_values(table=ref('version_usage_stats_to_stage_mappings') , column='stage') %}

WITH change AS (

  SELECT * FROM {{ref('version_usage_data_monthly_change')}}

),

booleans AS (

  SELECT * FROM {{ref('version_usage_data_boolean')}}

),

pings AS (

  SELECT
    change.*,
    booleans.active_user_count AS user_count
  FROM change
    LEFT JOIN booleans
      ON change.uuid = booleans.uuid
      AND DATE_TRUNC('month', change.created_at) = DATE_TRUNC('month', booleans.created_at)

),

hostnames AS (
    SELECT
      uuid,
      LISTAGG(DISTINCT hostname, ', ') AS uuid_hostnames
    FROM
      {{ref('version_usage_data_unpacked')}}
    GROUP BY 1
)

SELECT
  DATE_TRUNC('month', pings.created_at)::DATE AS created_at,
  pings.uuid,
  pings.ping_source,
  pings.main_edition,
  pings.edition_type,
  uuid_hostnames,

  -- For each stage in stage_list, pass it to the `stage_mapping` macro
  {% for stage_name in stage_list %}
    {{ stage_mapping( stage_name ) }} ,
  {% endfor %}

  SUM(user_count) AS "Total"
FROM pings
  LEFT JOIN hostnames
    ON pings.uuid = hostnames.uuid
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY 1