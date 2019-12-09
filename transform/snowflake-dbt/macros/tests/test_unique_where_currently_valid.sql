{% macro test_unique_where_currently_valid(model, column_name) %}

WITH data AS (

    SELECT
      {{ column_name }} AS id,
      COUNT(*)          AS count_valid_rows
    FROM {{ model }}
    WHERE is_currently_valid = True
    GROUP BY 1

)

SELECT COUNT(*)
FROM data
WHERE count_valid_rows != 1

{% endmacro %}
