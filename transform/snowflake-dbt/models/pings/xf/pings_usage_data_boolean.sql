-- depends_on: {{ ref('pings_usage_data') }}

{{get_pings_json_keys()}}

WITH usage_month as (

    SELECT * FROM {{ ref('pings_usage_data_month') }}

)

SELECT  DISTINCT *,
        {%- for row in load_result('stats_used')['data'] -%}
        {{ case_when_boolean_int(row[1]) }}                                       AS {{row[1]}}_active {{ "," if not loop.last }}
        {%- endfor -%}
FROM usage_month