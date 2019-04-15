{{ config(schema='analytics') }}

with raw_mrr_totals_levelled AS (

       SELECT * FROM {{ref('mrr_totals_levelled')}}

), mrr_totals_levelled AS (

      SELECT subscription_name, 
              subscription_name_slugify,
              sfdc_account_id,
              oldest_subscription_in_cohort,
              lineage,
              mrr_month,
              zuora_subscription_cohort_month,
              zuora_subscription_cohort_quarter,
              months_since_zuora_subscription_cohort_start,
              quarters_since_zuora_subscription_cohort_start,
              array_agg(DISTINCT product_category) WITHIN GROUP (ORDER BY product_category ASC) AS product_category,
              array_agg(DISTINCT UNIT_OF_MEASURE) WITHIN GROUP (ORDER BY unit_of_measure ASC) AS unit_of_measure,
              sum(quantity) as quantity,
              sum(mrr) as mrr
      FROM raw_mrr_totals_levelled
      {{ dbt_utils.group_by(n=10) }}

), list AS ( --get all the subscription + their lineage + the month we're looking for MRR for (12 month in the future)

       SELECT subscription_name_slugify   AS original_sub,
                     c.value::string      AS subscriptions_in_lineage,
                     mrr_month            AS original_mrr_month,
                     dateadd('year', 1, mrr_month) AS retention_month
       FROM mrr_totals_levelled,
       lateral flatten(input =>split(lineage, ',')) C
       {{ dbt_utils.group_by(n=4) }}

), retention_subs AS ( --find which of those subscriptions are real and group them by their sub you're comparing to.

       SELECT original_sub,
               product_category   AS original_product_category,
               retention_month,
               original_mrr_month,
               unit_of_measure    AS original_unit_of_measure,
               sum(quantity)      AS original_quantity,
               sum(mrr)           AS retention_mrr
       FROM list
       INNER JOIN mrr_totals_levelled 
       ON retention_month = mrr_month
       AND subscriptions_in_lineage = subscription_name_slugify
       {{ dbt_utils.group_by(n=5) }}

), finals AS (

       SELECT mrr_totals_levelled.*, retention_subs.*,
              CASE WHEN retention_subs.original_product_category = mrr_totals_levelled.product_category AND retention_subs.original_quantity < mrr_totals_levelled.quantity 
                    THEN 'Seat Expansion'
                   WHEN (retention_subs.original_product_category = mrr_totals_levelled.product_category AND retention_subs.original_quantity = mrr_totals_levelled.quantity 
                    OR retention_subs.original_product_category = mrr_totals_levelled.product_category AND retention_subs.original_quantity > mrr_totals_levelled.quantity) 
                   THEN 'Discount/Price Change'
                   WHEN retention_subs.original_product_category != mrr_totals_levelled.product_category AND retention_subs.original_quantity = mrr_totals_levelled.quantity 
                    THEN 'Product Change'
                   WHEN retention_subs.original_product_category != mrr_totals_levelled.product_category AND retention_subs.original_quantity != mrr_totals_levelled.quantity 
                    THEN 'Product Change/Seat Change Mix'
                ELSE 'Unknown' END AS expansion_type
       FROM mrr_totals_levelled
       LEFT JOIN retention_subs
       ON subscription_name_slugify = original_sub
       AND retention_subs.original_mrr_month = mrr_totals_levelled.mrr_month
       WHERE mrr_totals_levelled.mrr > retention_subs.retention_mrr

), joined as (

      SELECT subscription_name              AS zuora_subscription_name,
             oldest_subscription_in_cohort  AS zuora_subscription_id,
             dateadd('year', 1, mrr_month)  AS retention_month, --THIS IS THE RETENTION MONTH, NOT THE MRR MONTH!!
             expansion_type,
             original_product_category,
             product_category,
             original_quantity,
             quantity,
             original_unit_of_measure,
             unit_of_measure
      FROM finals

)

SELECT *
FROM joined
WHERE retention_month <= dateadd(month, -1, CURRENT_DATE)
