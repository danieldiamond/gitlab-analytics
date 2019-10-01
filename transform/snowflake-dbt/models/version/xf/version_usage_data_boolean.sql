{% set version_stats_list = dbt_utils.get_column_values(table=ref('version_list'), column='full_ping_name', max_records=1000) %}

WITH usage_month as (

    SELECT * FROM {{ ref('pings_usage_data_month') }}

)

SELECT  DISTINCT *,

        {% for ping_name in p_list %}
        {{ case_when_boolean_int( ping_name ) }}                                       AS {{ping_name}}_active {{ "," if not loop.last }}
        {% endfor %} 
   
FROM usage_month