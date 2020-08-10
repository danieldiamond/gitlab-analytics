WITH sfdc_account AS (

    SELECT *
    FROM {{ ref('sfdc_account_source') }}
    WHERE account_id IS NOT NULL

), sfdc_users AS (

    SELECT *
    FROM {{ ref('sfdc_users_source') }}

), ultimate_parent_account AS (

    SELECT
      account_id,
      account_name,
      account_segment,
      billing_country,
      df_industry,
      account_owner_team,
      tsp_territory
    FROM sfdc_account
    WHERE account_id = ultimate_parent_account_id

), deleted_accounts AS (

    SELECT *
    FROM sfdc_account
    WHERE is_deleted = TRUE

), master_records AS (

    SELECT
      a.account_id,
      COALESCE(
      b.master_record_id, a.master_record_id) AS sfdc_master_record_id
    FROM deleted_accounts a
    LEFT JOIN deleted_accounts b
      ON a.master_record_id = b.account_id

)

SELECT
  sfdc_account.account_id                 AS crm_id,
  sfdc_account.account_name               AS customer_name,
  sfdc_account.billing_country            AS customer_country,
  ultimate_parent_account.account_id      AS ultimate_parent_account_id,
  ultimate_parent_account.account_name    AS ultimate_parent_account_name,
  CASE
     WHEN ultimate_parent_account.account_segment = 'Unknown' THEN 'SMB'
     WHEN ultimate_parent_account.account_segment IS NULL     THEN 'SMB'
     ELSE ultimate_parent_account.account_segment
  END                                     AS ultimate_parent_account_segment,
  ultimate_parent_account.billing_country AS ultimate_parent_billing_country,
  ultimate_parent_account.df_industry     AS ultimate_parent_industry,
  CASE
    WHEN ultimate_parent_account.account_owner_team = 'US East'                                                                                       THEN 'US East'
    WHEN ultimate_parent_account.account_owner_team = 'US West'                                                                                       THEN 'US West'
    WHEN ultimate_parent_account.account_owner_team = 'EMEA'                                                                                          THEN 'EMEA'
    WHEN ultimate_parent_account.account_owner_team = 'APAC'                                                                                          THEN 'APAC'
    WHEN ultimate_parent_account.account_owner_team = 'Public Sector'                                                                                 THEN 'Public Sector'
    WHEN ultimate_parent_account.account_owner_team IN ('Commercial', 'Commercial - MM', 'MM - East', 'MM - West', 'MM-EMEA', 'MM - EMEA', 'MM-APAC') THEN 'MM'
    WHEN ultimate_parent_account.account_owner_team IN ('SMB', 'SMB - US', 'SMB - International', 'Commercial - SMB')                                 THEN 'SMB'
    ELSE 'Other'
  END                                     AS ultimate_parent_account_owner_team,
  ultimate_parent_account.tsp_territory   AS ultimate_parent_territory,
  sfdc_account.record_type_id             AS record_type_id,
  sfdc_account.gitlab_entity,
  sfdc_account.federal_account            AS federal_account,
  sfdc_account.gitlab_com_user,
  sfdc_account.account_owner,
  sfdc_account.account_owner_team,
  sfdc_account.account_type,
  sfdc_users.name                         AS technical_account_manager,
  sfdc_account.is_deleted                 AS is_deleted,
  CASE
    WHEN sfdc_account.is_deleted
      THEN master_records.sfdc_master_record_id
    ELSE NULL
  END                                     AS merged_to_account_id
FROM sfdc_account
LEFT JOIN master_records
  ON sfdc_account.account_id = master_records.account_id
LEFT JOIN ultimate_parent_account
  ON ultimate_parent_account.account_id = sfdc_account.ultimate_parent_account_id
LEFT OUTER JOIN sfdc_users
  ON sfdc_account.technical_account_manager_id = sfdc_users.id
