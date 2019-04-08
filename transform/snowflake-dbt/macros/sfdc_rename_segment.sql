{%- macro sfdc_rename_segment(column_1, column_2) -%}

 CASE WHEN (contains(COALESCE({{ column_1 }}, {{ column_2 }}), 'Strategic')) THEN 'Strategic'
      WHEN (contains(COALESCE({{ column_1 }}, {{ column_2 }}), 'Large')) THEN 'Large'
      WHEN (contains(COALESCE({{ column_1 }}, {{ column_2 }}), 'Unknown')) THEN 'Unknown'
      WHEN (contains(COALESCE({{ column_1 }}, {{ column_2 }}), 'Mid-Market')) THEN 'Mid-Market'
      WHEN (contains(COALESCE({{ column_1 }}, {{ column_2 }}), 'Smb')) THEN 'SMB'
      ELSE 'Unknown' 
  END
{%- endmacro -%}




