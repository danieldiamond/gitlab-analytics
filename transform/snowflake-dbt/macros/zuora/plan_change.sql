{%- macro plan_change(original_product_ranking, new_product_ranking) -%}

    CASE
        WHEN {{ original_product_ranking }} = 0
            THEN 'Not Valid'
        WHEN {{ original_product_ranking }} = {{ new_product_ranking }}
            THEN 'Maintained'
        WHEN {{ original_product_ranking }} > {{ new_product_ranking }}
            THEN 'Downgraded'
        WHEN {{ original_product_ranking }} < {{ new_product_ranking }}
            THEN 'Upgraded'
        END                                       AS plan_change

{%- endmacro -%}
