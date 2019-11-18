{% set version_usage_stats_list = dbt_utils.get_column_values(table=ref('version_usage_stats_list'), column='full_ping_name', max_records=1000) %}

WITH usage_month as (

    SELECT * FROM {{ ref('version_usage_data_month') }}

)

SELECT DISTINCT
  *,

  {% for ping_name in version_usage_stats_list %}
  {{ case_when_boolean_int( ping_name ) }} AS {{ping_name}}_active
    {%- if not loop.last %}      
    ,
    {% endif -%}
  {% endfor %}

FROM usage_month
