{% macro schema_union_limit(schema_part, table_name, column_name, day_limit=30) %}

WITH base_union AS (

    {{ schema_union_all(schema_part, table_name) }}

) 

SELECT *
FROM base_union
WHERE {{ column_name }} >= dateadd('day', -{{ day_limit }}, CURRENT_DATE())

{% endmacro %}
