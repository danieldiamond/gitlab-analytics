{% macro quarters_diff(start_month, end_month) %}

((DATE_PART('year', date_trunc('quarter', {{end_month}})::date::date) - DATE_PART('year', {{start_month}}::date)) * 12 +
(DATE_PART('month', date_trunc('quarter', {{end_month}})::date::date) - DATE_PART('month', {{start_month}}::date)))/3

{% endmacro %}

