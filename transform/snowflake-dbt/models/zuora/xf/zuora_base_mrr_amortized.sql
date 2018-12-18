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
           subscription_slug_for_counting,
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
  

)

SELECT 
       account_number,
       subscription_name,
       subscription_name_slugify,
       subscription_slug_for_counting,
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
GROUP BY 1, 2, 3, 4, 5, 6, 8, 9, 10, 11
