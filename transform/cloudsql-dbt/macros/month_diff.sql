{% macro month_diff(start_month, end_month) %}

date_part('year', age({{end_month}}::TIMESTAMP, {{start_month}}::TIMESTAMP)) * 12 +
date_part('month', age({{end_month}}::TIMESTAMP, {{start_month}}::TIMESTAMP)) 

{% endmacro %}

