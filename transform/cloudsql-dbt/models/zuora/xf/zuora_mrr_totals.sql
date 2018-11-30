{{
  config({
    "materialized":"table",
    "post-hook": [
       "DROP INDEX IF EXISTS {{ this.schema }}.index_zuora_mrr_totals_account_number",
       "CREATE INDEX IF NOT EXISTS index_zuora_mrr_totals_account_number ON {{ this }}(account_number)"
    ]
  })
}}


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
      cohort_month AS zuora_subscription_cohort_month,
      cohort_quarter AS zuora_subscription_cohort_quarter,
      {{ month_diff('cohort_month ', 'trueup_month') }} as months_since_zuora_subscription_cohort_start,
      charge_name AS mrr_type,
      null as quantity,
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
      {{ month_diff('cohort_month ', 'mrr_month') }} as months_since_zuora_subscription_cohort_start,
      rate_plan_charge_name AS mrr_type,
      quantity,
      mrr
    FROM base_mrr

  ), uniqueified as (

    SELECT max(account_number) as account_number,
            max(subscription_name_slugify) as subscription_name_slugify,
            max(subscription_name) as subscription_name,
            subscription_slug_for_counting,
            mrr_month,
            min(zuora_subscription_cohort_month) as zuora_subscription_cohort_month,
            min(zuora_subscription_cohort_quarter) as zuora_subscription_cohort_quarter,
            max(months_since_zuora_subscription_cohort_start) as months_since_zuora_subscription_cohort_start,
            --mrr_type,
            --quantity,
            sum(mrr) as mrr 
    FROM mrr_combined
    GROUP BY 4, 5

), subs_with_retention_values as (

    SELECT
      last_year.subscription_slug_for_counting,
      (last_year.mrr_month + INTERVAL '12 month')::date as mrr_month,
      last_year.zuora_subscription_cohort_month,
      last_year.zuora_subscription_cohort_quarter,
      (last_year.months_since_zuora_subscription_cohort_start + 12) as months_since_zuora_subscription_cohort_start,
      current.mrr as mrr,
      last_year.mrr::float as mrr_12mo_ago
    FROM uniqueified as last_year --12 months ago
    LEFT JOIN uniqueified as current --now
    ON last_year.subscription_slug_for_counting = current.subscription_slug_for_counting
    AND (last_year.mrr_month + INTERVAL '12 month')::date = current.mrr_month

), first_12months as (

    SELECT
      subscription_slug_for_counting,
      mrr_month,
      zuora_subscription_cohort_month,
      zuora_subscription_cohort_quarter,
      months_since_zuora_subscription_cohort_start,
      mrr,
      null::float as mrr_12mo_ago
    FROM uniqueified
    WHERE months_since_zuora_subscription_cohort_start < 12


), unioned as (

    SELECT * FROM subs_with_retention_values
    UNION ALL
    SELECT * FROM first_12months

)

SELECT *
FROM unioned