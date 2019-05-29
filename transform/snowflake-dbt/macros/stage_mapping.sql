{% macro stage_mapping(stage) %} 

    {%- call statement('get_mappings', fetch_result=True) %}

        SELECT stats_used_key_name
        FROM {{ ref('ping_metrics_to_stage_mapping_data') }} 
        WHERE stage = '{{ stage }}'

    {%- endcall -%}

    {%- set value_list = load_result('get_mappings') -%}

    {%- if value_list and value_list['data'] -%}

        {%- set values = value_list['data'] | map(attribute=0) | list %}

        COALESCE(
            SUM(
                CASE WHEN
                    {% for feature in values %}

                        pings.{{ feature }}_change > 0

                        {%- if not loop.last %}
                            OR
                        {% else %}
                            THEN pings.user_count END
                        {% endif -%}

                    {% endfor -%} 
            )
        , 0)
     {%- else -%}
        {{ return(1) }}
     {%- endif %}

    AS {{ stage }}_sum

 {% endmacro %}
