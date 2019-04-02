-- depends_on: {{ ref('pings_usage_data') }}

{{get_pings_json_keys()}}

WITH pings_mom_change as (

  SELECT  md5(uuid || created_at)             AS unique_key,
          uuid                                AS uuid,
          edition                             AS edition,
          main_edition                        AS main_edition,
          edition_type                        AS edition_type,

          {%- for row in load_result('stats_used')['data'] -%}
          {{ monthly_change(row[1]) }},
          {%- endfor -%}

          {{ monthly_change('active_user_count') }},
          created_at                          AS created_at
  FROM {{ ref("pings_usage_data_month") }}

)

SELECT * FROM pings_mom_change
