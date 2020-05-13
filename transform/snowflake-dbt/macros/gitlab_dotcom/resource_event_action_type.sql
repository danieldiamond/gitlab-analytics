{%- macro resource_event_action_type(action) -%}

  CASE  
    WHEN {{action}}::INTEGER = 1 THEN 'added'
    WHEN {{action}}::INTEGER = 2 THEN 'removed'
  END

{%- endmacro -%}
