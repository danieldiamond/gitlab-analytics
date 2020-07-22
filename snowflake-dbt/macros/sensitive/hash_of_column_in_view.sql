{% macro hash_of_column_in_view(column) %}

    {% set results = run_query("select ENCRYPT('{{ get_salt(column|lower) }}', '{{ get_salt(column|lower) }}')::VARCHAR") %}

    sha2(
        TRIM(
            LOWER(
                {{column|lower}} ||  '{{results.columns[0].values()[0]}}'
            )
        )
    ) AS {{column|lower}}_hash,

{% endmacro %}