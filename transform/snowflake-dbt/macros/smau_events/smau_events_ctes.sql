{%- macro smau_events_ctes(action_name, regexp_where_statements=[]) -%}

{{action_name}} AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start) AS event_date,
    page_url_path,
    '{{action_name}}'       AS event_type,
    page_view_id

  FROM snowplow_page_views
  WHERE TRUE 
  {{ log(regexp_where_statements, info=True) }}

  {% for regexp_where_statement in regexp_where_statements %}
    AND page_url_path {{regexp_where_statement.regexp_function}} '{{regexp_where_statement.regexp}}'
  {% endfor %}

)

{%- endmacro -%}
