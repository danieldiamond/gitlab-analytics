{%- macro account_owner_team(column_1) -%}

CASE
  WHEN ultimate_parent_account.account_owner_team = 'US East'                                                                                       THEN 'US East'
  WHEN ultimate_parent_account.account_owner_team = 'US West'                                                                                       THEN 'US West'
  WHEN ultimate_parent_account.account_owner_team = 'EMEA'                                                                                          THEN 'EMEA'
  WHEN ultimate_parent_account.account_owner_team = 'APAC'                                                                                          THEN 'APAC'
  WHEN ultimate_parent_account.account_owner_team = 'Public Sector'                                                                                 THEN 'Public Sector'
  WHEN ultimate_parent_account.account_owner_team IN ('Commercial', 'Commercial - MM', 'MM - East', 'MM - West', 'MM-EMEA', 'MM - EMEA', 'MM-APAC') THEN 'MM'
  WHEN ultimate_parent_account.account_owner_team IN ('SMB', 'SMB - US', 'SMB - International', 'Commercial - SMB')                                 THEN 'SMB'
  ELSE 'Other'
END

{%- endmacro -%}
