{% macro nohash_sensitive_columns(source_table, join_key) %}

    {% set meta_columns = get_meta_columns(source_table, "sensitive") %}

    sha2({{join_key|lower}}) AS {{join_key|lower}}_hash,
    
    {% for column in meta_columns %}
    
    {{column|lower}}  {% if not loop.last %} , {% endif %}
    
    {% endfor %}

{% endmacro %}
