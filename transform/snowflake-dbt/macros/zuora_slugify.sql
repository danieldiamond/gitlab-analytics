{%- macro zuora_slugify(input_text) -%}
--  This macro replaces replaces any combination of whitespace and 2 pipes with a single pipe (important for renewal subscriptions) and it replaces all non alphanumeric characters with dashes and casts it to lowercases as well. The end result of using this macro on data like "A-S00003830 || A-S00013333" is "a-s00003830|a-s00013333"
    trim(
        lower(
            regexp_replace(
                regexp_replace(
                    {{ input_text }}
                , '\\s+\\|{2}\\s+', '|')
            , '[^A-Za-z0-9|]', '-')
            )
        )

{%- endmacro -%}
