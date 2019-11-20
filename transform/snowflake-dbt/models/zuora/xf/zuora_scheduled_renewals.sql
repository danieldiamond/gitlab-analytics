WITH zuora_base_mrr AS (

    SELECT *
    FROM {{ref('zuora_base_mrr')}}

), zuora_account AS (

    SELECT *
    FROM {{ref('zuora_account')}}

), sfdc_accounts_xf AS (

    SELECT *
    FROM {{ref('sfdc_accounts_xf')}}

), sfdc_deleted_accounts AS (

    SELECT *
    FROM {{ref('sfdc_deleted_accounts')}}

), initial_join_to_sfdc AS (

    SELECT
      zuora_base_mrr.*,
      CASE
        WHEN lower(zuora_base_mrr.rate_plan_name) LIKE '%support%'
          THEN 'Support Only'
        ELSE 'Full Service'
      END                           AS service_type,
      zuora_account.account_id      AS zuora_account_id,
      zuora_account.account_name    AS zuora_account_name,
      zuora_account.crm_id          AS zuora_crm_id,
      sfdc_accounts_xf.account_id   AS sfdc_account_id_int
    FROM zuora_base_mrr
    LEFT JOIN zuora_account
      ON zuora_account.account_number = zuora_base_mrr.account_number
    LEFT JOIN sfdc_accounts_xf
      ON sfdc_accounts_xf.account_id = zuora_account.crm_id
    WHERE subscription_status = 'Active'
      AND exclude_from_renewal_report != 'Yes'
      --AND date_trunc('year', effective_end_date)::DATE >= date_trunc('year', CURRENT_DATE)::DATE

), replace_sfdc_account_id_with_master_record_id AS (

    SELECT
      coalesce(initial_join_to_sfdc.sfdc_account_id_int, sfdc_master_record_id) AS sfdc_account_id,
      initial_join_to_sfdc.*
    FROM initial_join_to_sfdc
    LEFT JOIN sfdc_deleted_accounts
      ON initial_join_to_sfdc.zuora_crm_id = sfdc_deleted_accounts.sfdc_account_id

), joined as (

    SELECT
      replace_sfdc_account_id_with_master_record_id.country           AS zuora_sold_to_country,
      replace_sfdc_account_id_with_master_record_id.account_number    AS zuora_account_number,
      replace_sfdc_account_id_with_master_record_id.subscription_name_slugify,
      replace_sfdc_account_id_with_master_record_id.subscription_name,
      replace_sfdc_account_id_with_master_record_id.oldest_subscription_in_cohort,
      replace_sfdc_account_id_with_master_record_id.lineage,
      replace_sfdc_account_id_with_master_record_id.effective_start_date,
      replace_sfdc_account_id_with_master_record_id.effective_end_date,
      replace_sfdc_account_id_with_master_record_id.subscription_start_date,
      replace_sfdc_account_id_with_master_record_id.exclude_from_renewal_report,
      replace_sfdc_account_id_with_master_record_id.mrr,
      replace_sfdc_account_id_with_master_record_id.zuora_account_id,
      replace_sfdc_account_id_with_master_record_id.zuora_account_name,
      replace_sfdc_account_id_with_master_record_id.product_category,
      replace_sfdc_account_id_with_master_record_id.delivery,
      replace_sfdc_account_id_with_master_record_id.rate_plan_name,
      replace_sfdc_account_id_with_master_record_id.service_type,
      replace_sfdc_account_id_with_master_record_id.unit_of_measure,
      replace_sfdc_account_id_with_master_record_id.quantity,
      sfdc_accounts_xf.account_id                                      AS sfdc_account_id,
      sfdc_accounts_xf.account_name                                    AS sfdc_account_name,
      sfdc_accounts_xf.ultimate_parent_account_id,
      sfdc_accounts_xf.ultimate_parent_account_name,
      sfdc_accounts_xf.ultimate_parent_account_segment,
      sfdc_accounts_xf.account_segment,
      sfdc_accounts_xf.next_renewal_date,
      sfdc_accounts_xf.account_region,
      sfdc_accounts_xf.account_sub_region,
      sfdc_accounts_xf.account_owner_team
    FROM replace_sfdc_account_id_with_master_record_id
    LEFT JOIN sfdc_accounts_xf
      ON sfdc_accounts_xf.account_id = replace_sfdc_account_id_with_master_record_id.sfdc_account_id

)

SELECT *
FROM joined
