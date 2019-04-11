{%- macro sfdc_rename_segment(column_1, column_2) -%}

coalesce({{ column_1 }}, {{ column_2 }}) AS {{ column_1 }}_new_col, 
CASE  WHEN lower({{ column_1 }}_new_col) = 'smb' THEN 'SMB' 
      WHEN lower({{ column_1 }}_new_col) = 'mid-market' THEN 'Mid-Market' 
      WHEN {{ column_1 }}_new_col is not null THEN initcap({{ column_1 }}_new_col) 
      ELSE 'Unknown'
END

{%- endmacro -%}




