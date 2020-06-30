{%- macro get_current_date() -%}

    {% set dt = modules.datetime.datetime.now() %}
    {{ return(dt) }}

{%- endmacro -%}
