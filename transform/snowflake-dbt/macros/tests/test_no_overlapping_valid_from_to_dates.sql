{% macro test_no_overlapping_valid_from_to_dates(model, column_name) %}

{% set dates_to_check = ('2013-01-01', '2016-02-02', '2017-03-03', '2018-04-04', '2019-05-05', '2020-06-06') %}

WITH data AS (

    SELECT
      {{ column_name }} AS id,
      valid_from,
      valid_to
    FROM {{ model }}

), grouped AS (

    {% for date in dates_to_check %}
    SELECT
      id,
      COUNT(*) AS count_rows_valid_on_date
    FROM data
      INNER JOIN (SELECT 1) AS a
        ON '{{ date }}'::DATE BETWEEN data.valid_from AND COALESCE(data.valid_to, '9999-12-31')
     GROUP BY 1
    {{ "UNION" if not loop.last }}
    {% endfor %}

 )

SELECT COUNT(*)
FROM grouped
WHERE count_rows_valid_on_date != 1


{% endmacro %}