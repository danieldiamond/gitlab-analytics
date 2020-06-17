{% macro hash_sensitive_columns(source_table) %}

    {% set meta_columns = get_meta_columns(source_table, "sensitive") %}

    {%- for column in meta_columns %}
    
        {{ hash_of_column(column) }}
    
    {% endfor %}

    {{ dbt_utils.star(from=ref(source_table), except=meta_columns) }}

{% endmacro %}
