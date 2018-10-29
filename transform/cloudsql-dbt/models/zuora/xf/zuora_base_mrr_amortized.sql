WITH zuora_mrr AS (

    SELECT *
    FROM {{ ref('zuora_base_mrr') }}

), date_table AS (

     SELECT *
     FROM {{ ref('date_details') }}

), amortized_mrr AS (

    SELECT account_number,
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
           LEFT JOIN LATERAL (
        SELECT date_actual
        FROM date_table d
        WHERE day_of_month = 1
          AND d.date_actual BETWEEN b.effective_start_month AND b.effective_end_month
        ) m on TRUE

)

SELECT *
FROM amortized_mrr
WHERE mrr_month IS NOT NULL
