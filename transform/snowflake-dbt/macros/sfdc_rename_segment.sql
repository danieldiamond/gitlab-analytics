{%- macro sfdc_rename_segment(column_1, column_2) -%}
  CASE WHEN COALESCE(column_1, column_2) in ('smb', 'Smb') THEN 'SMB'
       WHEN COALESCE(column_1, column_2) in ('large', 'strategic', 'unknown') THEN initcap(COALESCE(column_1, column_2))
       WHEN COALESCE(column_1, column_2) in ('mid-market', 'Mid-market') THEN 'Mid-Market'
   END 
{%- endmacro -%}