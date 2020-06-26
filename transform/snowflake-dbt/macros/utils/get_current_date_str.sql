{%- macro get_current_date_str() -%}

    {% set dt = modules.datetime.now().date() %}
    {{ dt }}

{%- endmacro -%}
