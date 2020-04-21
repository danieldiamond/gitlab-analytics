WITH sfdc_account AS (

    SELECT *
    FROM {{ ref('sfdc_account_source') } }
    WHERE account_id IS NOT NULL

), sfdc_users AS (

    SELECT *
    FROM {{ ref('sfdc_users_source') }}

), sfdc_record_type AS (

    SELECT *
    FROM {{ ref('sfdc_record_type_source') }}

), ultimate_parent_account AS (

    SELECT
      account_id,
      account_name,
      billing_country
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
    FROM deleted_accounts a LEFT JOIN deleted_accounts b
    ON a.master_record_id = b.account_id

)

SELECT
  sfdc_account.account_id                 AS customer_id,
  sfdc_account.account_name               AS customer_name,
  sfdc_account.billing_country            AS customer_country,
  ultimate_parent_account.account_id      AS ultimate_parent_account_id,
  ultimate_parent_account.account_name    AS ultimate_parent_account_name,
  ultimate_parent_account.billing_country AS ultimate_parent_billing_country,
  sfdc_account.record_type_id             AS record_type_id,
  sfdc_account.gitlab_entity,
  sfdc_account.federal_account            AS federal_account,
  sfdc_account.gitlab_com_user,
  sfdc_account.account_owner,
  sfdc_account.account_owner_team,
  sfdc_account.account_type,
  sfdc_users.name                         AS technical_account_manager,
  sfdc_record_type.record_type_name,
  sfdc_record_type.business_process_id,
  sfdc_record_type.record_type_label,
  sfdc_record_type.record_type_description,
  sfdc_record_type.record_type_modifying_object_type,
  sfdc_account.is_deleted                 AS is_deleted,
  CASE
    WHEN sfdc_account.is_deleted
      THEN master_records.sfdc_master_record_id
    ELSE NULL
    END                                   AS merged_to_account_id
FROM sfdc_account
     LEFT JOIN master_records ON sfdc_account.account_id = master_records.account_id
     LEFT JOIN ultimate_parent_account ON ultimate_parent_account.account_id = sfdc_account.ultimate_parent_account_id
     LEFT OUTER JOIN sfdc_users ON sfdc_account.technical_account_manager_id = sfdc_users.id
     LEFT JOIN sfdc_record_type ON sfdc_account.record_type_id = sfdc_record_type.record_type_id



