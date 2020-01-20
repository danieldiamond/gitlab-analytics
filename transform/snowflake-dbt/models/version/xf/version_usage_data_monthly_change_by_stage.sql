-- depends_on: ref('version_usage_stats_to_stage_mappings')
{% set stage_list = dbt_utils.get_column_values(table=ref('version_usage_stats_to_stage_mappings') , column='stage', default=[]) %}

WITH monthly_change AS (

  SELECT *
  FROM {{ref('version_usage_data_monthly_change')}}

),

booleans AS (

  SELECT *
  FROM {{ref('version_usage_data_boolean')}}

),

change AS (

  SELECT
    monthly_change.*,
    booleans.active_user_count AS user_count
  FROM monthly_change
    LEFT JOIN booleans
      ON monthly_change.uuid = booleans.uuid
      AND DATE_TRUNC('month', monthly_change.created_at) = DATE_TRUNC('month', booleans.created_at)

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
  DATE_TRUNC('month', change.created_at)::DATE AS created_at,
  change.uuid,
  change.ping_source,
  change.main_edition,
  change.edition_type,
  uuid_hostnames,

  -- For each stage in stage_list, pass it to the `stage_mapping` macro
  {% for stage_name in stage_list %}
    {{ stage_mapping( stage_name ) }} ,
  {% endfor %}

  SUM(user_count) AS "Total"
FROM change
  LEFT JOIN hostnames
    ON change.uuid = hostnames.uuid
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY 1
