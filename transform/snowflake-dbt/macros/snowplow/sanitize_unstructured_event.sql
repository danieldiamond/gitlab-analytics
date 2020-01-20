{% macro sanitize_unstructured_event() -%}

  CASE
    WHEN event_name IN ('submit_form', 'focus_form', 'change_form')
      THEN 'masked'
    ELSE nullif(jsontext['unstruct_event']::STRING, '')   
  END

{%- endmacro %}
