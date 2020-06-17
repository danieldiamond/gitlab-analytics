{% macro hash_of_column(column) %}

    sha2(TRIM(LOWER({{column|lower}} || '{{ env_var("salt") }}'))) AS {{column|lower}}_hash,

{% endmacro %}