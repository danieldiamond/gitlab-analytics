{{ config({
    "schema": "analytics",
    "post-hook": "grant select on {{this}} to role reporter"
    })
}}

{% set stage_list = dbt_utils.get_column_values(table=ref('ping_metrics_to_stage_mapping_data'), column='stage') %}

with change as (

  SELECT * FROM {{ref('pings_usage_data_monthly_change')}}

), 

boolean as (

  SELECT * FROM {{ref('pings_usage_data_boolean')}}

),

pings AS (

  SELECT
    change.*,
    boolean.active_user_count AS user_count
  FROM change
    LEFT JOIN boolean
      ON change.uuid = boolean.uuid
      AND DATE_TRUNC('month', change.created_at) = DATE_TRUNC('month', boolean.created_at)

),

hostnames AS (
    SELECT
      uuid
      , LISTAGG(DISTINCT hostname, ', ') AS uuid_hostnames
    FROM
      {{ref('pings_usage_data_unpacked')}}
    GROUP BY
      1
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
  LEFT JOIN hostnames ON pings.uuid = hostnames.uuid
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY 1