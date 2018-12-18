{%- macro zuora_slugify(input_text) -%}
 
replace(lower( {{input_text}} ), ' ', '-')

{%- endmacro -%}
