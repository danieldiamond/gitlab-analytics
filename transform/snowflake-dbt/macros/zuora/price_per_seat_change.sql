{%- macro price_per_seat_change(original_mrr, original_seat_quantity, original_unit_of_measure,
                                new_mrr, new_seat_quantity, new_unit_of_measure) -%}

CASE 
    WHEN NOT (       -- Only compare prices per seat when the unit of measure of the original and new plans is seats
        ARRAY_CONTAINS('Seats'::variant, {{ original_unit_of_measure }}) AND
        ARRAY_SIZE({{ original_unit_of_measure }}) = 1 AND
        ARRAY_CONTAINS('Seats'::variant, {{ new_unit_of_measure }}) AND
        ARRAY_SIZE({{ new_unit_of_measure }}) = 1
    )
        THEN NULL
    ELSE
      	( {{ new_mrr }} / {{ new_seat_quantity }} ) - ( {{ original_mrr }} / {{ original_seat_quantity }})	
    END AS price_per_seat_change

{%- endmacro -%}
