WITH zuora_mrr AS (

    SELECT *
    FROM {{ ref('zuora_base_mrr') }}

), date_table AS (

     SELECT *
     FROM {{ ref('date_details') }}
     WHERE day_of_month = 1

), amortized_mrr AS (

    SELECT 
           account_number,
           subscription_name,
           subscription_name_slugify,
           oldest_subscription_in_cohort,
           lineage,
           rate_plan_name,
           rate_plan_charge_name,
           mrr,
           date_actual as mrr_month,
           sub_start_month,
           sub_end_month,
           effective_start_month,
           effective_end_month,
           effective_start_date,
           effective_end_date,
           cohort_month,
           cohort_quarter,
           unit_of_measure,
           quantity
  FROM zuora_mrr b
  LEFT JOIN date_table d
  ON d.date_actual >= b.effective_start_month
  AND d.date_actual <= b.effective_end_month
  

), almost_final as (

SELECT 
       account_number,
       subscription_name,
       subscription_name_slugify,
       oldest_subscription_in_cohort,
       lineage,
       rate_plan_name,
       rate_plan_charge_name,
       sum(mrr) as mrr,
       mrr_month,
       cohort_month,
       cohort_quarter,
       unit_of_measure,
       sum(quantity) as quantity
FROM amortized_mrr
WHERE mrr_month IS NOT NULL
GROUP BY 1, 2, 3, 4, 5, 6, 7, 9, 10, 11, 12

), final as (

SELECT * , 
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
      ELSE 'Other' END AS product_category
FROM almost_final

)

SELECT * 
FROM final
