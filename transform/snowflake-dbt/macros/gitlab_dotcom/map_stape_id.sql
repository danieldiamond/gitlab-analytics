{%- macro map_state_id(state_id) -%}

    CASE
      WHEN state_id = 1 THEN 'opened'
      WHEN state_id = 2 THEN 'closed'
      WHEN state_id = 3 THEN 'merged'
      WHEN state_id = 4 THEN 'locked'
      ELSE NULL
    END             

{%- endmacro -%}
