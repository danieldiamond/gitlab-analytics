{% macro hash_of_column(column) %}

    sha2(
        TRIM(
            LOWER(
                {{column|lower}} || ENCRYPT('{{ get_salt(column|lower) }}', '{{ get_salt(column|lower) }}')::VARCHAR 
            )
        )
    ) AS {{column|lower}}_hash,

{% endmacro %}