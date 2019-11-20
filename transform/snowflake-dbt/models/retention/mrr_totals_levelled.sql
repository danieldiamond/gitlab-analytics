
with zuora_mrr_totals AS (

    SELECT * FROM {{ref('zuora_mrr_totals')}}

), zuora_account AS (

    SELECT * FROM {{ref('zuora_account')}}

), sfdc_accounts_xf AS (

    SELECT * FROM {{ref('sfdc_accounts_xf')}}

), sfdc_deleted_accounts AS (

    SELECT * FROM {{ref('sfdc_deleted_accounts')}}

), initial_join_to_sfdc AS (

    SELECT
           zuora_mrr_totals.*,
           zuora_account.account_id AS zuora_account_id,
           zuora_account.account_name AS zuora_account_name,
           zuora_account.crm_id as zuora_crm_id,
           sfdc_accounts_xf.account_id as sfdc_account_id_int
    FROM zuora_mrr_totals
    LEFT JOIN zuora_account
    ON zuora_account.account_number = zuora_mrr_totals.account_number
    LEFT JOIN sfdc_accounts_xf
    ON sfdc_accounts_xf.account_id = zuora_account.crm_id

), replace_sfdc_account_id_with_master_record_id AS (

    SELECT coalesce(initial_join_to_sfdc.sfdc_account_id_int, sfdc_master_record_id) AS sfdc_account_id,
          initial_join_to_sfdc.*
    FROM initial_join_to_sfdc
    LEFT JOIN sfdc_deleted_accounts
    ON initial_join_to_sfdc.zuora_crm_id = sfdc_deleted_accounts.sfdc_account_id

), joined as (

    SELECT
           replace_sfdc_account_id_with_master_record_id.country   AS zuora_sold_to_country,
           replace_sfdc_account_id_with_master_record_id.account_number   AS zuora_account_number,
           replace_sfdc_account_id_with_master_record_id.subscription_name_slugify,
           replace_sfdc_account_id_with_master_record_id.subscription_name,
           replace_sfdc_account_id_with_master_record_id.oldest_subscription_in_cohort,
           replace_sfdc_account_id_with_master_record_id.lineage,
           replace_sfdc_account_id_with_master_record_id.mrr_month,
           replace_sfdc_account_id_with_master_record_id.zuora_subscription_cohort_month,
           replace_sfdc_account_id_with_master_record_id.zuora_subscription_cohort_quarter,
           replace_sfdc_account_id_with_master_record_id.mrr,
           replace_sfdc_account_id_with_master_record_id.months_since_zuora_subscription_cohort_start,
           replace_sfdc_account_id_with_master_record_id.quarters_since_zuora_subscription_cohort_start,
           replace_sfdc_account_id_with_master_record_id.zuora_account_id,
           replace_sfdc_account_id_with_master_record_id.zuora_account_name,
           replace_sfdc_account_id_with_master_record_id.product_category,
           replace_sfdc_account_id_with_master_record_id.delivery,
           replace_sfdc_account_id_with_master_record_id.rate_plan_name,
           replace_sfdc_account_id_with_master_record_id.service_type,
           replace_sfdc_account_id_with_master_record_id.unit_of_measure,
           replace_sfdc_account_id_with_master_record_id.quantity,
           replace_sfdc_account_id_with_master_record_id.subscription_status,
           replace_sfdc_account_id_with_master_record_id.exclude_from_renewal_report,
           replace_sfdc_account_id_with_master_record_id.sub_end_month,
           sfdc_accounts_xf.account_id                                      AS sfdc_account_id,
           sfdc_accounts_xf.account_name                                    AS sfdc_account_name,
           sfdc_accounts_xf.ultimate_parent_account_id,
           sfdc_accounts_xf.ultimate_parent_account_name,
           min(zuora_subscription_cohort_month) OVER (
              PARTITION BY zuora_account_id)                                AS zuora_account_cohort_month,
           min(zuora_subscription_cohort_quarter) OVER (
              PARTITION BY zuora_account_id)                                AS zuora_account_cohort_quarter,
           min(zuora_subscription_cohort_month) OVER (
              PARTITION BY sfdc_accounts_xf.account_id)                     AS sfdc_account_cohort_month,
           min(zuora_subscription_cohort_quarter) OVER (
              PARTITION BY sfdc_accounts_xf.account_id)                     AS sfdc_account_cohort_quarter,
           min(zuora_subscription_cohort_month) OVER (
              PARTITION BY ultimate_parent_account_id)                      AS parent_account_cohort_month,
           min(zuora_subscription_cohort_quarter) OVER (
              PARTITION BY ultimate_parent_account_id)                      AS parent_account_cohort_quarter
    FROM replace_sfdc_account_id_with_master_record_id
    LEFT JOIN sfdc_accounts_xf
    ON sfdc_accounts_xf.account_id = replace_sfdc_account_id_with_master_record_id.sfdc_account_id

)

SELECT *,
      datediff(month, zuora_account_cohort_month, mrr_month) as months_since_zuora_account_cohort_start,
      datediff(quarter, zuora_account_cohort_quarter, mrr_month) as quarters_since_zuora_account_cohort_start,
      datediff(month, sfdc_account_cohort_month, mrr_month) as months_since_sfdc_account_cohort_start,
      datediff(quarter, sfdc_account_cohort_quarter, mrr_month) as quarters_since_sfdc_account_cohort_start,
      datediff(month, parent_account_cohort_month, mrr_month) as months_since_parent_account_cohort_start,
      datediff(quarter, parent_account_cohort_quarter, mrr_month) as quarters_since_parent_account_cohort_start
FROM joined
