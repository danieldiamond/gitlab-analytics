with mrr_totals_levelled AS (

       SELECT * FROM {{ref('mrr_totals_levelled')}}

), current_arr_segmentation_all_levels AS (

       SELECT * FROM {{ref('current_arr_segmentation_all_levels')}}
       WHERE level_ = 'zuora_subscription_id'

), mapping AS (
      
       SELECT  subscription_name, sfdc_account_id
       FROM mrr_totals_levelled
       GROUP BY 1, 2

), list AS ( --get all the subscription + their lineage + the month we're looking for MRR for (12 month in the future)

       SELECT subscription_name_slugify AS original_sub,
                     c.value::string AS subscriptions_in_lineage,
                     mrr_month as original_mrr_month,
                     dateadd('year', 1, mrr_month) AS retention_month
       FROM mrr_totals_levelled,
       lateral flatten(input =>split(lineage, ',')) C
       GROUP BY 1, 2, 3, 4

), retention_subs AS ( --find which of those subscriptions are real and group them by their sub you're comparing to.

       SELECT original_sub,
               retention_month,
               original_mrr_month,
               sum(mrr) AS retention_mrr
       FROM list
       INNER JOIN mrr_totals_levelled AS subs
       ON retention_month = mrr_month
       AND subscriptions_in_lineage = subscription_name_slugify
       GROUP BY 1, 2, 3

), finals AS (

       SELECT coalesce(retention_subs.retention_mrr, 0) AS net_retention_mrr,
              CASE WHEN net_retention_mrr > 0 
                  THEN least(net_retention_mrr, mrr)
                  ELSE 0 END AS gross_retention_mrr,
              retention_month,
              mrr_totals_levelled.*
       FROM mrr_totals_levelled
       LEFT JOIN retention_subs
       ON subscription_name_slugify = original_sub
       AND retention_subs.original_mrr_month = mrr_totals_levelled.mrr_month

), joined as (

      SELECT finals.subscription_name as zuora_subscription_name,
             finals.oldest_subscription_in_cohort as zuora_subscription_id,
             mapping.sfdc_account_id as salesforce_account_id,
             dateadd('year', 1, finals.mrr_month) AS retention_month, --THIS IS THE RETENTION MONTH, NOT THE MRR MONTH!!
             finals.mrr as original_mrr,
             finals.net_retention_mrr,
             finals.gross_retention_mrr,
             finals.zuora_subscription_cohort_month,
             finals.zuora_subscription_cohort_quarter,
             finals.months_since_zuora_subscription_cohort_start,
             finals.quarters_since_zuora_subscription_cohort_start,
             {{churn_type()}}
      FROM finals
      LEFT JOIN mapping
      ON mapping.subscription_name = finals.subscription_name

)

SELECT joined.*, 
        current_arr_segmentation_all_levels.arr_segmentation
FROM joined
LEFT JOIN current_arr_segmentation_all_levels
ON joined.zuora_subscription_id = current_arr_segmentation_all_levels.id
WHERE retention_month <= dateadd(month, -1, CURRENT_DATE)
