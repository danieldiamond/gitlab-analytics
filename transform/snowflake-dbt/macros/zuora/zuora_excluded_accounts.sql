{% macro zuora_excluded_accounts() %}

    {%- call statement('get_excluded_accounts', fetch_result=True) %}

        SELECT DISTINCT account_id
        FROM {{ref('zuora_excluded_accounts')}}
        WHERE account_id IS NOT NULL

    {%- endcall -%}

    {%- set value_list = load_result('get_excluded_accounts') -%}

    {# {{ log("Value List: " ~ value_list['data'], True) }} #}
    {%- set values = [] -%}
    {%- if value_list and value_list['data'] -%}
      {# {{%- set values = value_list['data'] | map(attribute=0) | join(', ') %}  #}
      {% for account_id in value_list['data'] | map(attribute=0) %}
         '{{account_id}}' 
        {%- if not loop.last %} , {%- endif %}
      {% endfor %}
    {%- endif -%}

 {% endmacro %}
