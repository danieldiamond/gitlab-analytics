{%- macro resource_label_action_type(resource_label_action_type_id) -%}

  CASE  WHEN {{resource_label_action_type_id}}::INTEGER = 1 THEN 'added'
        WHEN {{resource_label_action_type_id}}::INTEGER = 2 THEN 'removed'
  END

{%- endmacro -%}
