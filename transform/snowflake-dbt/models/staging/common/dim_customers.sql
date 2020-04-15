/*
 Parent: An accumulation of Accounts under an organization with common ownership.
 In the case of the U.S. government, we count U.S. government major agencies as a unique parent account.
 (In Salesforce this is the Ultimate Parent Account field)
 */

WITH sfdc_account AS (

    SELECT *
    FROM {{ ref('sfdc_account_source') }}

), sfdc_users AS (

    SELECT *
    FROM {{ ref('sfdc_users_source') }}

), sfdc_record_type AS (

    SELECT *
    FROM {{ ref('sfdc_record_type_source') }}

) joined AS (

    SELECT
      sfdc_account.account_id AS customer_id,
      sfdc_account.account_name AS customer_name,
      sfdc_account.record_type_id AS record_type_id,
      sfdc_account.gitlab_entity
      sfdc_account.federal_account AS federal_account,
      sfdc_account.gitlab_com_user,
      sfdc_account.account_owner,
      sfdc_account.account_owner_team,
      sfdc_account.account_type,
      sfdc_users.name AS technical_account_manager,
      sfdc_record_type.record_type_name,
      sfdc_record_type.business_process_id,
      sfdc_record_type.record_type_label,
      sfdc_record_type.record_type_description,
      sfdc_record_type.record_type_modifying_object_type
    FROM sfdc_account
    LEFT OUTER JOIN sfdc_users
      ON sfdc_account.technical_account_manager_id = sfdc_users.id
    LEFT JOIN sfdc_record_type
      ON sfdc_account.record_type_id = sfdc_record_type.record_type_id
    where account_id = ULTIMATE_PARENT_ACCOUNT_ID
)

SELECT *
FROM joined

