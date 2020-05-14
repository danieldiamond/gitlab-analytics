WITH zuora_account AS (

    SELECT * 
    FROM {{ref('zuora_account')}}

)

, sfdc_accounts_xf AS (

    SELECT * 
    FROM {{ref('sfdc_accounts_xf')}}

)

, sfdc_deleted_accounts AS (

    SELECT * 
    FROM {{ref('sfdc_deleted_accounts')}}

)

, initial_join_to_sfdc AS (

    SELECT
      zuora_account.account_id AS zuora_account_id,
      zuora_account.account_name AS zuora_account_name,
      zuora_account.crm_id as zuora_crm_id,
      sfdc_accounts_xf.account_id as sfdc_account_id_int
    FROM zuora_account
    LEFT JOIN sfdc_accounts_xf
    ON sfdc_accounts_xf.account_id = zuora_account.crm_id

)

, replace_sfdc_account_id_with_master_record_id AS (

    SELECT 
      COALESCE(initial_join_to_sfdc.sfdc_account_id_int, sfdc_master_record_id) AS sfdc_account_id,
      initial_join_to_sfdc.zuora_account_id,
      initial_join_to_sfdc.zuora_account_name,
      initial_join_to_sfdc.zuora_crm_id
    FROM initial_join_to_sfdc
    LEFT JOIN sfdc_deleted_accounts
    ON initial_join_to_sfdc.zuora_crm_id = sfdc_deleted_accounts.sfdc_account_id

)

SELECT *
FROM replace_sfdc_account_id_with_master_record_id
