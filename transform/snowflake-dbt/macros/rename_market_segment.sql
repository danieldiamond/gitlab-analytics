{%- macro rename_market_segment(column_name) -%}

  CASE WHEN (contains({{ column_name }}, 'smb')) THEN 'SMB'
  	   WHEN (contains({{ column_name }}, 'mid-market' OR 'Mid-market')) THEN 'Mid-Market'
       WHEN (contains({{ column_name }}, 'strategic' OR 'large' OR 'unknown'))  THEN initcap({{ column_name }})
       WHEN (contains({{ column_name }}, 'Strategic')) THEN 'Strategic'
       WHEN (contains({{ column_name }}, 'Large')) THEN 'Large'
       WHEN (contains({{ column_name }}, 'Unknown')) THEN 'Unknown'
       ELSE 'Unknown'
   END

{%- endmacro -%}
