{% macro hash_sensitive_columns(source_table) %}

    {% set meta_columns = get_meta_columns(source_table, "sensitive") %}

    {%- for column in meta_columns %}
    
    sha2({{column|lower}}) AS {{column|lower}}_hash,
    
    {% endfor %}

    {{ dbt_utils.star(from=ref(source_table), except=meta_columns) }}

{% endmacro %}
