WITH base_mrr AS ( 

    SELECT * FROM {{ ref('zuora_base_mrr_amortized') }}

), trueup_mrr AS (

    SELECT * FROM {{ ref('zuora_base_trueups') }}

), mrr_combined AS ( --union the two tables

    SELECT account_number,
          subscription_name_slugify,
          subscription_name,
          oldest_subscription_in_cohort,
          lineage,
          trueup_month AS mrr_month,
          cohort_month AS zuora_subscription_cohort_month,
          cohort_quarter AS zuora_subscription_cohort_quarter,
          mrr, 
          null AS product_category
    FROM trueup_mrr

    UNION ALL

    SELECT account_number,
          subscription_name_slugify,
          subscription_name,
          oldest_subscription_in_cohort,
          lineage,
          mrr_month,
          cohort_month AS zuora_subscription_cohort_month,
          cohort_quarter AS zuora_subscription_cohort_quarter,
          mrr,
          product_category
    FROM base_mrr

), uniqueified as ( -- one row per sub slug for counting x product_category x mrr_month combo, with first of other values

    SELECT {{ dbt_utils.surrogate_key('mrr_month', 'subscription_name_slugify', 'product_category') }} as primary_key,
          account_number,
          subscription_name_slugify,
          subscription_name,
          oldest_subscription_in_cohort,
          lineage,
          mrr_month,
          zuora_subscription_cohort_month,
          zuora_subscription_cohort_quarter,
          product_category,
          sum(mrr) as mrr
    FROM mrr_combined
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10

) 

SELECT *, -- calculate new values
      datediff(month,zuora_subscription_cohort_month, mrr_month) as months_since_zuora_subscription_cohort_start,
      datediff(quarter, zuora_subscription_cohort_quarter, mrr_month) as quarters_since_zuora_subscription_cohort_start
FROM uniqueified
