{{
  config( materialized='incremental',
    incremental_startegy='merge',
    unique_key='edw_customer_id')
  }}

WITH customers_db_customers AS (

    SELECT
        customer_id         AS customer_db_customer_id,
        customer_created_at AS customer_db_created_at,
        sfdc_account_id
    FROM ref( {{ 'customer_db_customers' }} )

), sfdc_accounts AS (

    SELECT
        account_id AS sfdc_account_id,
        created_at AS sfdc_created_at
    FROM ref( {{ 'sfdc_account' }} )
)

SELECT
  {{ dbt_utils.surrogate_key(
      'customer_db_customer_id',
      'sfdc_account_id'
  ) }}                                              AS edw_customer_id,
   customers_db_customers.customer_db_customer_id,
   sfdc_accounts.sfdc_account_id,
   customers_db_customers.customer_db_created_at,
   sfdc_accounts.sfdc_created_at
FROM customers_db_customers
OUTER JOIN sfdc_accounts
  ON customers_db_customers.sfdc_account_id = sfdc_accounts.sfdc_account_id

