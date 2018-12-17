{%- macro zuora_slugify(input_text) -%}


regexp_replace(trim(lower(regexp_replace( {{input_text}}, '[^\w\s-]', '-', 'i'))), '[\s]+', '-', 'i')
     

{%- endmacro -%}
