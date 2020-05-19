{%- macro map_state_id(state_id) -%}

    CASE
      WHEN {{ state_id }}::INTEGER = 1 THEN 'opened'
      WHEN {{ state_id }}::INTEGER = 2 THEN 'closed'
      WHEN {{ state_id }}::INTEGER = 3 THEN 'merged'
      WHEN {{ state_id }}::INTEGER = 4 THEN 'locked'
      ELSE NULL
    END             

{%- endmacro -%}
