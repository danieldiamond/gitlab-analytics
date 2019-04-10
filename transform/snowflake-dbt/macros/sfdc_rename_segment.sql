{%- macro sfdc_rename_segment(column_1, column_2) -%}

coalesce(column_1, column_2) as new_col, 
CASE  WHEN new_col = 'smb' OR new_col = 'Smb' OR new_col = 'SMB' THEN 'SMB' 
      WHEN new_col = 'mid-market' OR new_col = 'Mid-market' OR new_col = 'Mid-Market' THEN 'Mid-Market' 
      WHEN new_col is not null THEN initcap(new_col) 
      ELSE 'Unknown'
END AS new_col
{%- endmacro -%}




