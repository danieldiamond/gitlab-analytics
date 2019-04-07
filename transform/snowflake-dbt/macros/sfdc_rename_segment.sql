{%- macro sfdc_rename_segment(column_1, column_2) -%}
  COALESCE(initcap(COALESCE(column_1, column_2)), 'Unknown')       AS coalesced_segment

  CASE WHEN coalesced_segment, 'smb')) THEN 'SMB'
  	   WHEN coalesced_segment, 'mid-market' OR 'Mid-market')) THEN 'Mid-Market'
       WHEN coalesced_segment, 'strategic' OR 'large' OR 'unknown'))  THEN initcap({{ column_name }})
       WHEN coalesced_segment, 'Strategic')) THEN 'Strategic'
       WHEN coalesced_segment, 'Large')) THEN 'Large'
   END 

{%- endmacro -%}


