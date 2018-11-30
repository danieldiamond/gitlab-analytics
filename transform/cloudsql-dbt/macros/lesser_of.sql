{% macro lesser_of(value1, value2) %}

CASE WHEN {{ value1 }} < {{ value2 }}
                 THEN {{ value1 }} 
                 ELSE {{ value2 }} END
     

{% endmacro %}
