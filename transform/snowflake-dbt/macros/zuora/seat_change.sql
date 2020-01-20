{%- macro seat_change(original_seat_quantity, new_seat_quantity) -%}

    CASE
        WHEN {{ original_seat_quantity }} = {{ new_seat_quantity }}
            THEN 'Maintained'
        WHEN {{ original_seat_quantity }} > {{ new_seat_quantity }}
            THEN 'Contraction'
        WHEN {{ original_seat_quantity }} < {{ new_seat_quantity }}
            THEN 'Expansion'
        END                                       AS seat_change

{%- endmacro -%}
