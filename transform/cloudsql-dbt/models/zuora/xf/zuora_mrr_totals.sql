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
      cohort_month,
      cohort_quarter,
      {{ month_diff('cohort_month ', 'trueup_month') }} as months_since_cohort_start,
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
      cohort_month,
      cohort_quarter,
      {{ month_diff('cohort_month ', 'mrr_month') }} as months_since_cohort_start,
      rate_plan_charge_name AS mrr_type,
      quantity,
      mrr
    FROM base_mrr

  )

SELECT *, 
        CASE WHEN mrr_type != 'Trueup' THEN (lag(mrr, 12) over (partition by subscription_slug_for_counting
         order by mrr_month)) ELSE NULL END as mrr_12mo_ago,
        {{ quarters_diff('cohort_quarter', 'mrr_month') }} as quarters_since_cohort_start

FROM mrr_combined