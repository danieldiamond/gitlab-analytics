{%- macro sales_segment_cleaning(column_1) -%}
 
CASE  WHEN lower({{ column_1 }}) = 'smb' THEN 'SMB' 
      WHEN lower({{ column_1 }}) = 'mid-market' THEN 'Mid-Market' 
      WHEN lower({{ column_1 }}) = 'mid market' THEN 'Mid-Market' 
      WHEN {{ column_1 }} is not null THEN initcap({{ column_1 }}) 
END

{%- endmacro -%}