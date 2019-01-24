{%- macro zuora_slugify(input_text) -%}
--  This macro replaces all non alphanumeric characters with dashes and lowercases it as well.
    trim(
        lower(
            regexp_replace( {{ input_text }}, '[^A-Za-z0-9]', '-')
            )
        )

{%- endmacro -%}
