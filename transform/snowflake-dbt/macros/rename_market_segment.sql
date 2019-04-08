{%- macro rename_market_segment(column_name) -%}

  CASE WHEN (contains({{ column_name }}, 'Strategic')) THEN 'Strategic'
       WHEN (contains({{ column_name }}, 'Large')) THEN 'Large'
       WHEN (contains({{ column_name }}, 'Unknown')) THEN 'Unknown'
       WHEN (contains({{ column_name }}, 'smb')) THEN 'SMB'
       WHEN {{ column_name }} in ('large', 'strategic', 'unknown') THEN initcap({{ column_name }})
       WHEN {{ column_name }} in ('mid-market', 'Mid-market') THEN 'Mid-Market'
       ELSE 'Unknown'
   END

{%- endmacro -%}


