{% set version_usage_stats_list = dbt_utils.get_column_values(table=ref('version_usage_stats_list'), column='full_ping_name', max_records=1000, default=['']) %}


WITH usage_data_unpacked_intermediate AS (

    SELECT *
    FROM {{  ref('version_usage_data_unpacked_intermediate') }}

), transformed AS (

    SELECT
      id,
      {{ dbt_utils.star(from=ref('version_usage_data_unpacked_intermediate'), except=(version_usage_stats_list|upper)) }},
      {% for stat_name in version_usage_stats_list %}
        IFF({{stat_name}} = -1, NULL, {{stat_name}}) AS {{stat_name}}
        {{ "," if not loop.last }}
      {% endfor %}
    FROM usage_data_unpacked_intermediate

)

SELECT *
FROM transformed
