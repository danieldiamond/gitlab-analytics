WITH base_mrr AS (

    SELECT *
    FROM {{ ref('zuora_base_mrr_amortized') }}

),

    trueup_mrr AS (

      SELECT *
      FROM {{ ref('zuora_base_trueups') }}

  ),

    mrr_combined AS (

    SELECT
      account_number,
      subscription_name_slugify,
      subscription_name,
      subscription_slug_for_counting,
      trueup_month AS mrr_month,
      cohort_month,
      date_part('year', age(trueup_month :: TIMESTAMP, cohort_month :: TIMESTAMP)) * 12 +
      date_part('month', age(trueup_month :: TIMESTAMP, cohort_month :: TIMESTAMP)) as months_since_cohort_start,
      charge_name AS mrr_type,
      mrr
    FROM trueup_mrr

    UNION ALL

    SELECT
      account_number,
      subscription_name_slugify,
      subscription_name,
      subscription_slug_for_counting,
      mrr_month,
      cohort_month,
      date_part('year', age(mrr_month :: TIMESTAMP, cohort_month :: TIMESTAMP)) * 12 +
      date_part('month', age(mrr_month :: TIMESTAMP, cohort_month :: TIMESTAMP)) as months_since_cohort_start,
      rate_plan_charge_name AS mrr_type,
      mrr
    FROM base_mrr

  )

SELECT *
FROM mrr_combined