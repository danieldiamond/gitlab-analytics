{%- macro action_type(action_type_id) -%}

  CASE  WHEN {{action_type_id}}::INTEGER = 1 THEN 'created'
        WHEN {{action_type_id}}::INTEGER = 2 THEN 'updated'
        WHEN {{action_type_id}}::INTEGER = 3 THEN 'closed'
        WHEN {{action_type_id}}::INTEGER = 4 THEN 'reopened'
        WHEN {{action_type_id}}::INTEGER = 5 THEN 'pushed'
        WHEN {{action_type_id}}::INTEGER = 6 THEN 'commented'
        WHEN {{action_type_id}}::INTEGER = 7 THEN 'merged'
        WHEN {{action_type_id}}::INTEGER = 8 THEN 'joined'
        WHEN {{action_type_id}}::INTEGER = 9 THEN 'left'
        WHEN {{action_type_id}}::INTEGER = 10 THEN 'destroyed'
        WHEN {{action_type_id}}::INTEGER = 11 THEN 'expired'
        END

{%- endmacro -%}
