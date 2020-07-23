{{
  config( materialized='incremental',
    incremental_startegy='merge',
    unique_key='customer_id')
  }}

WITH customers_db_customers AS (

    SELECT
        customer_id         AS customer_db_customer_id,
        customer_created_at AS customer_db_created_at,
        sfdc_account_id
    FROM {{ ref( 'customers_db_customers')  }}

), sfdc_accounts AS (

    SELECT *
    FROM {{ ref( 'sfdc_account' ) }}

), ultimate_parent_account AS (

    SELECT
      account_id,
      account_name,
      account_segment,
      billing_country
    FROM sfdc_account
    WHERE account_id = ultimate_parent_account_id

), deleted_accounts AS (

     SELECT *
    FROM {{ ref('sfdc_deleted_accounts' ) }}

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
  {{ dbt_utils.surrogate_key(
      'customer_db_customer_id',
      'sfdc_account_id') }}               AS customer_id,
  customers_db_customers.customer_db_customer_id,
  sfdc_accounts.sfdc_account_id,
  sfdc_account.account_name               AS customer_name,
  ultimate_parent_account.account_id      AS ultimate_parent_account_id,
  ultimate_parent_account.account_name    AS ultimate_parent_account_name,
  ultimate_parent_account.account_segment AS ultimate_parent_account_segment,
  sfdc_account.record_type_id             AS record_type_id,
  sfdc_account.gitlab_entity,
  sfdc_account.federal_account            AS federal_account,
  sfdc_account.gitlab_com_user,
  sfdc_account.account_owner,
  sfdc_account.account_owner_team,
  sfdc_account.account_type,
  CASE
    WHEN sfdc_account.is_deleted
      THEN master_records.sfdc_master_record_id
    ELSE NULL
  END                                     AS merged_to_account_id,
  customers_db_customers.customer_db_created_at,
  sfdc_accounts.sfdc_created_at
FROM sfdc_account
OUTER JOIN customers_db_customers
  ON customers_db_customers.sfdc_account_id = sfdc_accounts.sfdc_account_id
LEFT JOIN master_records
  ON sfdc_account.account_id = master_records.account_id
LEFT JOIN ultimate_parent_account
  ON ultimate_parent_account.account_id = sfdc_account.ultimate_parent_account_id
