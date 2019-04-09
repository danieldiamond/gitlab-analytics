{%- macro sfdc_rename_segment(column_1, column_2) -%}

 CASE WHEN (contains(COALESCE({{ column_1 }}, {{ column_2 }}, 'Unknown'), 'Smb')) THEN 'SMB'
      WHEN (contains(COALESCE({{ column_1 }}, {{ column_2 }}, 'Unknown'), 'SMB')) THEN 'SMB'
      WHEN (contains(COALESCE({{ column_1 }}, {{ column_2 }}, 'Unknown'), 'Strategic')) THEN 'Strategic'
      WHEN (contains(COALESCE({{ column_1 }}, {{ column_2 }}, 'Unknown'), 'strategic')) THEN 'Strategic'
      WHEN (contains(COALESCE({{ column_1 }}, {{ column_2 }}, 'Unknown'), 'Large')) THEN 'Large'
      WHEN (contains(COALESCE({{ column_1 }}, {{ column_2 }}, 'Unknown'), 'Unknown')) THEN 'Unknown'
      WHEN (contains(COALESCE({{ column_1 }}, {{ column_2 }}, 'Unknown'), 'Mid-Market')) THEN 'Mid-Market'
      WHEN (contains(COALESCE({{ column_1 }}, {{ column_2 }}, 'Unknown'), 'Mid-market')) THEN 'Mid-Market'

  END

{%- endmacro -%}




