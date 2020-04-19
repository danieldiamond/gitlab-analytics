WITH sfdc_account AS (

    SELECT *
    FROM {{ ref('sfdc_account_source') }}

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
)

    SELECT
      sfdc_account.account_id AS customer_id,
      sfdc_account.account_name AS customer_name,
      sfdc_account.billing_country AS customer_country,
      ultimate_parent_account.account_id AS ultimate_parent_account_id,
      ultimate_parent_account.account_name AS ultimate_parent_account_name,
      ultimate_parent_account.account_name AS ultimate_parent_billing_country,
      sfdc_account.record_type_id AS record_type_id,
      sfdc_account.gitlab_entity,
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
    LEFT JOIN ultimate_parent_account
        ON ultimate_parent_account.account_id = sfdc_account.ultimate_parent_account_id
    LEFT OUTER JOIN sfdc_users
        ON sfdc_account.technical_account_manager_id = sfdc_users.id
    LEFT JOIN sfdc_record_type
        ON sfdc_account.record_type_id = sfdc_record_type.record_type_id
    WHERE sfdc_account.account_id IS NOT NULL
     AND sfdc_account.is_deleted = FALSE


