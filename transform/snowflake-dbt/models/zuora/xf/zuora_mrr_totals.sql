WITH base_mrr AS (

    SELECT *
    FROM {{ ref('zuora_base_mrr_amortized') }}

),

    trueup_mrr AS (

      SELECT *
      FROM {{ ref('zuora_base_trueups') }}

  ),

    mrr_combined AS ( --union the two tables

    SELECT
      account_number,
      subscription_name_slugify,
      subscription_name,
      subscription_slug_for_counting,
      trueup_month AS mrr_month,
      cohort_month AS zuora_subscription_cohort_month,
      cohort_quarter AS zuora_subscription_cohort_quarter,
      mrr
    FROM trueup_mrr

    UNION ALL

    SELECT
      
      account_number,
      subscription_name_slugify,
      subscription_name,
      subscription_slug_for_counting,
      mrr_month,
      cohort_month AS zuora_subscription_cohort_month,
      cohort_quarter AS zuora_subscription_cohort_quarter,
      mrr
    FROM base_mrr

  ), uniqueified as ( -- one row per sub slug for counting x mrr_month combo, with first of other values

    SELECT FIRST_VALUE(account_number) OVER (PARTITION BY subscription_name_slugify
              ORDER BY mrr_month ASC ) AS account_number,
            FIRST_VALUE(subscription_name_slugify) OVER (PARTITION BY subscription_name_slugify
              ORDER BY mrr_month ASC ) AS subscription_name_slugify,
            FIRST_VALUE(subscription_name) OVER (PARTITION BY subscription_name_slugify
              ORDER BY mrr_month ASC ) AS subscription_name,
            subscription_slug_for_counting,
            mrr_month,
            min(zuora_subscription_cohort_month) as zuora_subscription_cohort_month,
            min(zuora_subscription_cohort_quarter) as zuora_subscription_cohort_quarter,
            sum(mrr) as mrr 
    FROM mrr_combined
    GROUP BY account_number, subscription_name_slugify, subscription_name, 4, 5

), subs_with_retention_values as ( --12 months ago to current (dynamically)

    SELECT
      last_year.account_number,
      last_year.subscription_name_slugify,
      last_year.subscription_name,
      last_year.subscription_slug_for_counting,
      dateadd(month, 12, last_year.mrr_month::date) as mrr_month,
      least(last_year.zuora_subscription_cohort_month, current_year.zuora_subscription_cohort_month) AS zuora_subscription_cohort_month,
      least(last_year.zuora_subscription_cohort_quarter, current_year.zuora_subscription_cohort_quarter) AS zuora_subscription_cohort_quarter,
      --(last_year.months_since_zuora_subscription_cohort_start + 12) as months_since_zuora_subscription_cohort_start,
      current_year.mrr as mrr,
      last_year.mrr::float as mrr_12mo_ago
    FROM uniqueified as last_year --12 months ago
    LEFT JOIN uniqueified as current_year --now
    ON last_year.subscription_slug_for_counting = current_year.subscription_slug_for_counting
    AND dateadd(month, 12, last_year.mrr_month::date) = current_year.mrr_month::date

), excluded as ( --prep list of accounts that were excluded from subs_with_retention_values

    SELECT
      uniqueified.*,
      null::float as mrr_12mo_ago
    FROM subs_with_retention_values
    RIGHT JOIN uniqueified
    ON uniqueified.subscription_slug_for_counting = subs_with_retention_values.subscription_slug_for_counting
    AND uniqueified.mrr_month = subs_with_retention_values.mrr_month
    WHERE subs_with_retention_values.subscription_slug_for_counting IS NULL
     AND subs_with_retention_values.mrr_month IS NULL


), unioned as ( -- union 

    SELECT * FROM subs_with_retention_values 
    UNION ALL
    SELECT * FROM excluded 

)

SELECT *, -- calculate new values
      datediff(month,'zuora_subscription_cohort_month ', 'mrr_month') as months_since_zuora_subscription_cohort_start,
      datediff(quarter, 'zuora_subscription_cohort_quarter', 'mrr_month') as quarters_since_zuora_subscription_cohort_start
FROM unioned
