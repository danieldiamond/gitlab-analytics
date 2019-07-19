{% macro unpack_unstructured_event(unstruct_columns_list, match_text, field_prefix) -%}

    {%- for column in unstruct_columns_list %}
      CASE
        WHEN try_parse_json(unstruct_event)['data']['schema'] LIKE '%{{ match_text }}%'
          THEN
            try_parse_json(unstruct_event)['data']['data']['{{ column }}']
          ELSE
            NULL
        END         AS {{ field_prefix }}_{{ column }}
        {%- if not loop.last %}
            ,
        {% endif %}
    {% endfor -%}

{%- endmacro %}
