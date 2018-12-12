{%- macro unnest(column, delimeter ="','") -%}

	trim(both ' ' from (unnest(string_to_array({{column}}, {{delimeter}})))) 

{%- endmacro -%}

{# input "Jane Doe, John Smith"; output "Jane Doe" "John Smith" on separate lines #}