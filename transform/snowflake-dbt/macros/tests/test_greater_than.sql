{% macro test_greater_than(model, compare_to, column_name) %}

WITH data AS (

    SELECT
      {{ column_name }}  AS greater_column,
      {{ compare_to }}   AS lesser_column
    FROM {{ model }}

)

SELECT COUNT(*)
FROM data
WHERE NOT greater_column >= lesser_column

{% endmacro %}
