WITH raw_mrr_totals_levelled AS (

       SELECT * FROM {{ref('mrr_totals_levelled')}}
       WHERE product_category != 'Trueup'

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
              DECODE(product_category,   --Need to account for the other categories
                'Bronze', 1,
                'Silver', 2,
                'Gold', 2,

                'Starter', 1,
                'Premium', 2,
                'Ultimate', 3,

                0
              )                                                                                 AS original_product_ranking,
              array_agg(DISTINCT product_category) WITHIN GROUP (ORDER BY product_category ASC) AS original_product_category,
              array_agg(DISTINCT delivery) WITHIN GROUP (ORDER BY delivery ASC)                 AS original_delivery,
              array_agg(DISTINCT UNIT_OF_MEASURE) WITHIN GROUP (ORDER BY unit_of_measure ASC)   AS original_unit_of_measure,

              sum(quantity) as original_quantity,
              sum(mrr) as original_mrr
      FROM raw_mrr_totals_levelled
      {{ dbt_utils.group_by(n=11) }}

), list AS ( --get all the subscription + their lineage + the month we're looking for MRR for (12 month in the future)

       SELECT subscription_name_slugify   AS original_sub,
                     c.value::string      AS subscriptions_in_lineage,
                     mrr_month            AS original_mrr_month,
                     dateadd('year', 1, mrr_month) AS retention_month
       FROM mrr_totals_levelled,
       lateral flatten(input =>split(lineage, ',')) C
       {{ dbt_utils.group_by(n=4) }}

), retention_subs AS ( --find which of those subscriptions are real and group them by their sub you're comparing to.

       SELECT    list.original_sub,
                 list.retention_month,
                 list.original_mrr_month,
                 mrr_totals_levelled.original_product_category      AS retention_product_category,
                 mrr_totals_levelled.original_delivery              AS retention_delivery,
                 mrr_totals_levelled.original_quantity              AS retention_quantity,
                 mrr_totals_levelled.original_unit_of_measure       AS retention_unit_of_measure,

                 mrr_totals_levelled.original_product_ranking       AS retention_product_ranking,
                 coalesce(sum(mrr_totals_levelled.original_mrr), 0) AS retention_mrr
       FROM list
       INNER JOIN mrr_totals_levelled
       ON retention_month = mrr_month
       AND subscriptions_in_lineage = subscription_name_slugify
       {{ dbt_utils.group_by(n=8) }}

), expansion AS (


       SELECT mrr_totals_levelled.*,
         retention_subs.*,
         coalesce(retention_subs.retention_mrr, 0) AS net_retention_mrr,
         CASE
           WHEN original_product_category = retention_product_category AND
                original_quantity < retention_quantity
             THEN 'Seat Expansion'
           WHEN (original_product_category = retention_product_category AND
                 original_quantity = retention_quantity
             OR original_product_category = retention_product_category AND
                original_quantity > retention_quantity)
             THEN 'Discount/Price Change'
           WHEN original_product_category != retention_product_category AND
                original_quantity = retention_quantity
             THEN 'Product Change'
           WHEN original_product_category != retention_product_category AND
                original_quantity != retention_quantity
             THEN 'Product Change/Seat Change Mix'
           ELSE 'Unknown' END                      AS churn_type,


         CASE
         WHEN original_product_category = retention_product_category AND
                 original_quantity < retention_quantity
             THEN 'Seat Change'
         WHEN (original_product_category = retention_product_category AND
                 original_quantity = retention_quantity
             OR original_product_category = retention_product_category AND
                 original_quantity > retention_quantity)
             THEN 'Price Change'
         WHEN original_product_ranking > retention_product_ranking AND   -- not sure of this
                 original_quantity = retention_quantity
             THEN 'Price Change/Product Change Mix'
         WHEN original_product_category != retention_product_category AND
                 original_quantity = retention_quantity
             THEN 'Product Change'
         WHEN original_product_category != retention_product_category AND
                 original_quantity != retention_quantity
             THEN 'Product Change/Seat Change Mix'
         ELSE 'Unknown' END                      AS retention_reason,

         CASE
                WHEN original_product_ranking = retention_product_ranking
                    THEN 'Maintained'
                WHEN original_product_ranking > retention_product_ranking
                    THEN 'Downgrade'
                WHEN original_product_ranking < retention_product_ranking
                    THEN 'Upgrade'
              END                                       AS plan_change,
              
              CASE
                WHEN original_quantity = retention_quantity
                    THEN 'Maintained'
                WHEN original_quantity > retention_quantity
                    THEN 'Expansion'
                WHEN original_quantity < retention_quantity
                    THEN 'Contraction'
              END                                       AS seat_change,
              
               price_change
       FROM mrr_totals_levelled
       LEFT JOIN retention_subs
       ON subscription_name_slugify = original_sub
       AND retention_subs.original_mrr_month = mrr_totals_levelled.mrr_month
       WHERE retention_mrr > original_mrr

), churn AS (

       SELECT mrr_totals_levelled.*,
              retention_subs.*,
              coalesce(retention_subs.retention_mrr, 0) AS net_retention_mrr,
              {{ churn_type('original_mrr', 'net_retention_mrr') }},
              CASE
                WHEN original_product_category = retention_product_category AND
                        original_quantity > retention_quantity
                  THEN 'Seat Change'
                WHEN (original_product_category = retention_product_category AND
                         original_quantity = retention_quantity
                  OR original_product_category = retention_product_category AND
                        original_quantity < retention_quantity)
                  THEN 'Price Change'
                WHEN original_product_ranking < retention_product_ranking AND   -- not sure of this
                 original_quantity = retention_quantity
                  THEN 'Price Change/Product Change Mix'
                WHEN original_product_category != retention_product_category AND
                        original_quantity = retention_quantity
                  THEN 'Product Change'
                WHEN original_product_category != retention_product_category AND
                        original_quantity != retention_quantity
                  THEN 'Product Change/Seat Change Mix'
                ELSE 'Unknown'
              END                                       AS retention_reason,

              CASE
                WHEN original_product_ranking = retention_product_ranking
                    THEN 'Maintained'
                WHEN original_product_ranking > retention_product_ranking
                    THEN 'Downgrade'
                WHEN original_product_ranking < retention_product_ranking
                    THEN 'Upgrade'
              END                                       AS plan_change,
              
              CASE
                WHEN original_quantity = retention_quantity
                    THEN 'Maintained'
                WHEN original_quantity > retention_quantity
                    THEN 'Contraction'
                WHEN original_quantity < retention_quantity
                    THEN 'Expansion'
              END                                       AS seat_change,
              
               price_change

       FROM mrr_totals_levelled
       LEFT JOIN retention_subs
        ON subscription_name_slugify = original_sub
            AND retention_subs.original_mrr_month = mrr_totals_levelled.mrr_month
       WHERE net_retention_mrr < original_mrr

), joined as (

      SELECT subscription_name              AS zuora_subscription_name,
             oldest_subscription_in_cohort  AS zuora_subscription_id,
             dateadd('year', 1, mrr_month)  AS retention_month, --THIS IS THE RETENTION MONTH, NOT THE MRR MONTH!!
             churn_type,
             original_product_category,
             retention_product_category,
             original_delivery,
             retention_delivery,
             original_quantity,
             retention_quantity,
             original_unit_of_measure,
             retention_unit_of_measure,
             original_mrr,
             net_retention_mrr as retention_mrr
      FROM expansion

      UNION ALL

      SELECT subscription_name              AS zuora_subscription_name,
             oldest_subscription_in_cohort  AS zuora_subscription_id,
             dateadd('year', 1, mrr_month)  AS retention_month, --THIS IS THE RETENTION MONTH, NOT THE MRR MONTH!!
             churn_type,
             original_product_category,
             retention_product_category,
             original_delivery,
             retention_delivery,
             original_quantity,
             retention_quantity,
             original_unit_of_measure,
             retention_unit_of_measure,
             original_mrr,
             net_retention_mrr as retention_mrr
      FROM churn

)

SELECT joined.*,
       rank() over(partition by zuora_subscription_id, churn_type, retention_reason, plan_change, seat_change
         order by retention_month asc)   AS rank_churn_type_month
FROM joined
WHERE retention_month <= dateadd(month, -1, CURRENT_DATE)
