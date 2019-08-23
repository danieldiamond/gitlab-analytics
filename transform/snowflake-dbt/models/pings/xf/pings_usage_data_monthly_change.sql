{% set ping_list = dbt_utils.get_column_values(table=ref('pings_list'), column='full_ping_name', max_records=1000) %}

WITH pings_mom_change as (

  SELECT  md5(uuid || created_at)             AS unique_key,
          uuid,
          created_at,
          ping_source,
          edition,
          main_edition,
          edition_type,
          {% for ping_name in ping_list %}
          {{ monthly_change(ping_name) }} {{ "," if not loop.last }}
          {% endfor %}


  FROM {{ ref("pings_usage_data_month") }}

)

SELECT * FROM pings_mom_change
