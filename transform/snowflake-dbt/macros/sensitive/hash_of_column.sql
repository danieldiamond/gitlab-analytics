{% macro hash_of_column(column) %}

    sha2(TRIM(LOWER({{column|lower}} || '{{ get_salt(column|lower) }}'))) AS {{column|lower}}_hash,

{% endmacro %}