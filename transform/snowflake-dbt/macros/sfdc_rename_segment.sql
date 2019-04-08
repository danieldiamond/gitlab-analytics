{%- macro sfdc_rename_segment(column_1, column_2) -%}
  COALESCE(initcap(COALESCE(column_1, column_2)), 'Unknown')       AS coalesced_segment

  CASE WHEN (contains(coalesced_segment, 'Strategic')) THEN 'Strategic'
       WHEN (contains(coalesced_segment, 'Large')) THEN 'Large'
       WHEN (contains(coalesced_segment, 'Unknown')) THEN 'Unknown'
       WHEN (contains(coalesced_segment, 'smb')) THEN 'SMB'
       WHEN coalesced_segment in ('large', 'strategic', 'unknown') THEN initcap(coalesced_segment)
       WHEN coalesced_segment in ('mid-market', 'Mid-market') THEN 'Mid-Market'

   END 

{%- endmacro -%}


