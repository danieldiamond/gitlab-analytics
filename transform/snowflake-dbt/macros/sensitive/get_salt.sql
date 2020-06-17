{% macro get_salt(column_name) %}

    {%- if 'name' in column_name -%}
        {{ return( env_var("NAME_SALT") ) }}
    {%- elif 'email' in column_name -%}
        {{ return( env_var("EMAIL_SALT") ) }}
    {%- elif 'ip' in column_name -%}
        {{ return( env_var("IP_SALT") ) }}
    {%- else -%}
        {{ return( env_var("SALT") ) }}
    {%- endif -%}

{% endmacro %}