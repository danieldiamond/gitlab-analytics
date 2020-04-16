WITH zuora_mrr_amortized AS (

    SELECT *
    FROM {{ ref('zuora_invoice_charges_mrr_amortized') }}

), sfdc_accounts_xf AS (

    SELECT *
    FROM {{ ref('sfdc_accounts_xf') }}

), sfdc_deleted_accounts AS (

    SELECT *
    FROM {{ ref('sfdc_deleted_accounts') }}

), initial_join_to_sfdc AS (

    SELECT
      zuora_mrr_amortized.*,
      sfdc_accounts_xf.account_id AS sfdc_account_id_int
    FROM zuora_mrr_amortized
    LEFT JOIN sfdc_accounts_xf
      ON zuora_mrr_amortized.crm_id = sfdc_accounts_xf.account_id

), final_join_to_sfdc AS (

    SELECT
      COALESCE(initial_join_to_sfdc.sfdc_account_id_int, sfdc_master_record_id) AS sfdc_account_id,
      initial_join_to_sfdc.*
    FROM initial_join_to_sfdc
    LEFT JOIN sfdc_deleted_accounts
      ON initial_join_to_sfdc.crm_id = sfdc_deleted_accounts.sfdc_account_id

), joined as (

    SELECT
      final_join_to_sfdc.mrr_month,
      final_join_to_sfdc.account_id                     AS zuora_account_id,
      final_join_to_sfdc.country                        AS zuora_sold_to_country,
      final_join_to_sfdc.account_name                   AS zuora_account_name,
      final_join_to_sfdc.account_number                 AS zuora_account_number,
      final_join_to_sfdc.subscription_id,
      final_join_to_sfdc.subscription_name_slugify,
      final_join_to_sfdc.subscription_status,
      final_join_to_sfdc.sub_start_month,
      final_join_to_sfdc.sub_end_month,
      final_join_to_sfdc.effective_start_month,
      final_join_to_sfdc.effective_end_month,
      final_join_to_sfdc.product_name,
      final_join_to_sfdc.rate_plan_charge_name,
      final_join_to_sfdc.rate_plan_name,
      final_join_to_sfdc.product_category,
      final_join_to_sfdc.delivery,
      final_join_to_sfdc.service_type,
      final_join_to_sfdc.unit_of_measure,
      final_join_to_sfdc.mrr,
      final_join_to_sfdc.quantity,
      sfdc_accounts_xf.account_id                       AS sfdc_account_id,
      sfdc_accounts_xf.account_name                     AS sfdc_account_name,
      sfdc_accounts_xf.ultimate_parent_account_id,
      sfdc_accounts_xf.ultimate_parent_account_name
    FROM final_join_to_sfdc
    LEFT JOIN sfdc_accounts_xf
      ON final_join_to_sfdc.sfdc_account_id = sfdc_accounts_xf.account_id

)

SELECT *
FROM joined
ORDER BY mrr_month DESC, ultimate_parent_account_name
