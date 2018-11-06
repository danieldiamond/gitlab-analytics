with zuora_mrr_totals as (

    SELECT * FROM {{ref('zuora_mrr_totals')}}

), zuora_account as (

    SELECT * FROM {{ref('zuora_account')}}

), sfdc_accounts_xf as (

    SELECT * FROM {{ref('sfdc_accounts_xf')}}

), joined as (

    SELECT md5(sub_rpc_id||(mrr_month :: varchar)) as sub_rpc_mrr_id,
           zuora_mrr_totals.*,
           zuora_account.account_id AS zuora_account_id,
           zuora_account.account_name AS zuora_account_name,
           sfdc_accounts_xf.account_id AS sfdc_account_id,
           sfdc_accounts_xf.account_name AS sfdc_account_name,
           sfdc_accounts_xf.ultimate_parent_account_id,
           sfdc_accounts_xf.ultimate_parent_account_name,
           row_number() OVER ( PARTITION BY ultimate_parent_account_id
              ORDER BY zuora_subscription_cohort_month DESC )              AS sub_row,
           min(zuora_subscription_cohort_month) OVER (
              PARTITION BY zuora_account.account_id)  AS zuora_account_cohort_month,
           min(zuora_subscription_cohort_quarter) OVER (
              PARTITION BY zuora_account.account_id)  AS zuora_account_cohort_quarter,
           min(zuora_subscription_cohort_month) OVER (
              PARTITION BY sfdc_accounts_xf.account_id)  AS sfdc_account_cohort_month,
           min(zuora_subscription_cohort_quarter) OVER (
              PARTITION BY sfdc_accounts_xf.account_id)  AS sfdc_account_cohort_quarter,
           min(zuora_subscription_cohort_month) OVER (
              PARTITION BY ultimate_parent_account_id)  AS parent_account_cohort_month,
           min(zuora_subscription_cohort_quarter) OVER (
              PARTITION BY ultimate_parent_account_id)  AS parent_account_cohort_quarter
    FROM zuora_mrr_totals
    LEFT JOIN zuora_account
        ON zuora_account.account_number = zuora_mrr_totals.account_number
    LEFT JOIN sfdc_accounts_xf
        ON sfdc_accounts_xf.account_id = zuora_account.crm_id

)

SELECT *,
      {{ month_diff('zuora_account_cohort_month', 'mrr_month') }} as months_since_zuora_account_cohort_start,
      {{ quarters_diff('zuora_account_cohort_month', 'mrr_month') }} as quarters_since_zuora_account_cohort_start,
      {{ month_diff('sfdc_account_cohort_month', 'mrr_month') }} as months_since_sfdc_account_cohort_start,
      {{ quarters_diff('sfdc_account_cohort_month', 'mrr_month') }} as quarters_since_sfdc_account_cohort_start,
      {{ month_diff('parent_account_cohort_month', 'mrr_month') }} as months_since_parent_account_cohort_start,
      {{ quarters_diff('parent_account_cohort_month', 'mrr_month') }} as quarters_since_parent_account_cohort_start

FROM joined