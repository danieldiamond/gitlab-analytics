{%- macro unnest(column, delimeter ="','") -%}

	trim(both ' ' from (unnest(string_to_array({{column}}, {{delimeter}})))) 

{%- endmacro -%}

