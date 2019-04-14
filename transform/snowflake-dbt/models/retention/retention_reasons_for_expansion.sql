WITH the_agg AS (
    SELECT
      subscription_name_slugify,
      oldest_subscription_in_cohort,
      rate_plan_name,
      mrr,
      mrr_month,
      unit_of_measure,
      quantity,
      CASE  WHEN lower(rate_plan_name) LIKE 'githost%' THEN 'GitHost'
            WHEN rate_plan_name IN ('#movingtogitlab', 'File Locking', 'Payment Gateway Test', 'Time Tracking', 'Training Workshop') THEN 'Other'
            WHEN lower(rate_plan_name) LIKE 'gitlab geo%' THEN 'Other'
            WHEN lower(rate_plan_name) LIKE 'basic%' THEN 'Basic'
            WHEN lower(rate_plan_name) LIKE 'bronze%' THEN 'Bronze'
            WHEN lower(rate_plan_name) LIKE 'ci runner%' THEN 'Other'
            WHEN lower(rate_plan_name) LIKE 'discount%' THEN 'Other'
            WHEN lower(rate_plan_name) LIKE '%premium%' THEN 'Premium'
            WHEN lower(rate_plan_name) LIKE '%starter%' THEN 'Starter'
            WHEN lower(rate_plan_name) LIKE '%ultimate%' THEN 'Ultimate'
            WHEN lower(rate_plan_name) LIKE 'gitlab enterprise edition%' THEN 'Starter'
            WHEN rate_plan_name IN ('GitLab Service Package', 'Implementation Services Quick Start', 'Implementation Support', 'Support Package') THEN 'Support'
            WHEN lower(rate_plan_name) LIKE 'gold%' THEN 'Gold'
            WHEN rate_plan_name = 'Pivotal Cloud Foundry Tile for GitLab EE' THEN 'Starter'
            WHEN lower(rate_plan_name) LIKE 'plus%' THEN 'Plus'
            WHEN lower(rate_plan_name) LIKE 'premium%' THEN 'Premium'
            WHEN lower(rate_plan_name) LIKE 'silver%' THEN 'Silver'
            WHEN lower(rate_plan_name) LIKE 'standard%' THEN 'Standard'
            WHEN rate_plan_name = 'Trueup' THEN 'Trueup'
      ELSE 'Other' END AS clean_name
    FROM analytics_staging.zuora_base_mrr_amortized
)

  , grouping AS (
    SELECT
      oldest_subscription_in_cohort,
      MRR_MONTH,
      array_agg(DISTINCT RATE_PLAN_NAME) WITHIN GROUP (ORDER BY rate_plan_name ASC) AS rpn,
      sum(MRR) AS mrr,
      array_agg(DISTINCT UNIT_OF_MEASURE) WITHIN GROUP (ORDER BY unit_of_measure ASC) AS uom,
      sum(QUANTITY) AS quantity,
      array_agg(DISTINCT clean_name) WITHIN GROUP (ORDER BY clean_name ASC) AS clean,
      array_agg(DISTINCT subscription_name_slugify) WITHIN GROUP (ORDER BY SUBSCRIPTION_NAME_SLUGIFY ASC) AS sub_count
    FROM the_agg
    GROUP BY 1, 2
    ORDER BY 1
)

  , the_old AS (

    SELECT *
    FROM grouping
    WHERE MRR_MONTH = '2017-07-01'
)

  , the_new AS (

    SELECT *
    FROM grouping
    WHERE MRR_MONTH = '2018-07-01'
)

  , changes AS (
    SELECT
      the_old.*,
      the_new.MRR_MONTH  AS new_mrr_month,
      the_new.rpn        AS new_rpn,
      the_new.mrr        AS new_mrr,
      the_new.uom        AS new_uom,
      the_new.quantity   AS new_quantity,
      the_new.clean      AS new_clean,
      the_new.sub_count  AS new_sub_count,
      CASE
      WHEN the_old.clean = the_new.clean AND the_old.quantity < the_new.quantity
        THEN 'Seat Expansion'
      WHEN
        (the_old.clean = the_new.clean AND the_old.quantity = the_new.quantity
         OR
         the_old.clean = the_new.clean AND the_old.quantity > the_new.quantity)
        THEN 'Discount/Price Change'
      WHEN the_old.clean != the_new.clean AND the_old.quantity = the_new.quantity
        THEN 'Product Change'
      WHEN the_old.clean != the_new.clean AND the_old.quantity != the_new.quantity
        THEN 'Product Change/Seat Change Mix'
      ELSE 'Unknown' END AS reason
    FROM the_old
      LEFT JOIN the_new ON the_old.SUBSCRIPTION_SLUG_FOR_COUNTING = the_new.SUBSCRIPTION_SLUG_FOR_COUNTING
    WHERE the_new.mrr > the_old.mrr
)

SELECT
--   *
  reason,
  count(*),
  round(count(*)/ sum(count(*)) over() * 100, 1) as percent
FROM changes
GROUP BY 1
order by 1
