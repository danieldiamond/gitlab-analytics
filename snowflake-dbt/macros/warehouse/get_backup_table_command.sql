{% macro get_backup_table_command(table, day_of_month) %}

    {% set backup_key -%}
        day_{{ day_of_month }}/{{ table.database.lower() }}/{{ table.schema.lower() }}/{{ table.name.lower() }}/data_
    {%- endset %}

    copy into @raw.public.backup_stage/{{ backup_key }}
    from {{ table.database }}.{{ table.schema }}."{{ table.name.upper() }}"
    header = true
    overwrite = true
    max_file_size = 1073741824;

{% endmacro %}
