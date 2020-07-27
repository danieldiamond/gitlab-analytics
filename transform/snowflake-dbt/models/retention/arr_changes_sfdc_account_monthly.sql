WITH mrr_totals_levelled AS (

    SELECT *
    FROM {{ ref('mrr_totals_levelled') }}

), sfdc_accounts_xf AS (

    SELECT *
    FROM {{ ref('sfdc_accounts_xf') }}

), monthly_arr AS (--Create a base data set of ARR and Customer Attributes to be used for the model

    SELECT
      mrr_month                                                                          AS arr_month,
      months_since_sfdc_account_cohort_start,
      quarters_since_sfdc_account_cohort_start,
      sfdc_account_cohort_quarter,
      sfdc_account_cohort_month,
      ultimate_parent_account_name,
      ultimate_parent_account_id,
      sfdc_account_name,
      sfdc_account_id,
      ARRAY_AGG(DISTINCT product_category) WITHIN GROUP (ORDER BY product_category ASC) AS product_category,
      ARRAY_AGG(DISTINCT delivery) WITHIN GROUP (ORDER BY delivery ASC)                 AS delivery,
      MAX(DECODE(product_category,   --Need to account for the 'other' categories
          'Bronze', 1,
          'Silver', 2,
          'Gold', 3,

          'Starter', 1,
          'Premium', 2,
          'Ultimate', 3,
          0
       ))                                                                               AS product_ranking,
      SUM(quantity)                                                                     AS quantity,
      SUM(mrr*12)                                                                       AS arr
    FROM mrr_totals_levelled
    {{ dbt_utils.group_by(n=9) }}

), previous_next_month AS (--Calculate the previous and next month's values for the required fields to be used for analysis

    SELECT
      monthly_arr.*,
       LAG(product_category) OVER (PARTITION BY sfdc_account_id ORDER BY arr_month)             AS previous_month_product_category,
       LAG(delivery) OVER (PARTITION BY sfdc_account_id ORDER BY arr_month)                     AS previous_month_delivery,
       COALESCE(LAG(product_ranking) OVER (PARTITION BY sfdc_account_id ORDER BY arr_month),0)  AS previous_month_product_ranking,
       COALESCE(LAG(quantity) OVER (PARTITION BY sfdc_account_id ORDER BY arr_month),0)         AS previous_month_quantity,
       COALESCE(LAG(arr) OVER (PARTITION BY sfdc_account_id ORDER BY arr_month),0)              AS previous_month_arr,
       COALESCE(LEAD(quantity) OVER (PARTITION BY sfdc_account_id ORDER BY arr_month),0)        AS next_month_quantity,
       COALESCE(LEAD(arr) OVER (PARTITION BY sfdc_account_id ORDER BY arr_month),0)             AS next_month_arr,
       COALESCE(LEAD(arr_month) OVER (PARTITION BY sfdc_account_id ORDER BY arr_month),DATEADD('month',1,arr_month))
                                                                                                AS next_month_arr_month
     FROM monthly_arr

), combined_arr AS (--The mrr_totals_levelled model does not have a record for when a subscription churns. This CTE creates a churn record with $0 ARR.

    SELECT
      arr_month,
      months_since_sfdc_account_cohort_start,
      quarters_since_sfdc_account_cohort_start,
      ultimate_parent_account_name,
      sfdc_account_name,
      sfdc_account_id,
      product_category,
      previous_month_product_category,
      delivery,
      previous_month_delivery,
      product_ranking,
      previous_month_product_ranking,
      quantity,
      previous_month_quantity,
      arr,
      previous_month_arr
    FROM previous_next_month

    UNION ALL

    SELECT
      next_month_arr_month                                                 AS arr_month,
      datediff(month, sfdc_account_cohort_month, next_month_arr_month)     AS months_since_sfdc_account_cohort_start,
      datediff(quarter, sfdc_account_cohort_quarter, next_month_arr_month) AS quarters_since_sfdc_account_cohort_start,
      ultimate_parent_account_name,
      sfdc_account_name,
      sfdc_account_id,
      NULL                                                                 AS product_category,
      previous_month_product_category,
      NULL                                                                 AS delivery,
      previous_month_delivery,
      NULL                                                                 AS product_ranking,
      previous_month_product_ranking,
      next_month_quantity                                                  AS quantity,
      previous_month_quantity,
      next_month_arr                                                       AS arr,
      previous_month_arr
    FROM previous_next_month
    WHERE next_month_arr = 0

), type_of_arr_change AS (--Calculate the type of ARR change

    SELECT
      combined_arr.*,
      CASE
        WHEN previous_month_arr = 0 AND arr > 0
          THEN 'New'
        WHEN arr = 0 AND previous_month_arr > 0
          THEN 'Churn'
	    WHEN arr < previous_month_arr AND arr > 0
          THEN 'Contraction'
	    WHEN arr > previous_month_arr
          THEN 'Expansion'
	    WHEN arr = previous_month_arr
          THEN 'No Impact'
	    ELSE NULL
	  END                     AS type_of_arr_change
  FROM combined_arr

), reason_for_arr_change_beg AS (--Create a record for the beginning ARR for each account by month

    SELECT
      arr_month,
      sfdc_account_id,
      previous_month_arr      AS beg_arr,
      previous_month_quantity AS beg_quantity
    FROM type_of_arr_change

), reason_for_arr_change_seat_change AS (--Calculate the change in ARR attributable to a change in seat quantity

    SELECT
      arr_month,
      sfdc_account_id,
      CASE
        WHEN previous_month_quantity != quantity AND previous_month_quantity > 0
          THEN ZEROIFNULL(previous_month_arr/NULLIF(previous_month_quantity,0) * (quantity - previous_month_quantity))
        WHEN previous_month_quantity != quantity AND previous_month_quantity = 0
          THEN arr
        ELSE 0
      END                     AS seat_change_arr,
      CASE
        WHEN previous_month_quantity != quantity
        THEN quantity - previous_month_quantity
        ELSE 0
      END                     AS seat_change_quantity
    FROM type_of_arr_change

), reason_for_arr_change_price_change AS (--For cases where the previous_month_product_category equals the current month product_category,
                                          --calculate the change in ARR attributable to a change in price per seat. There are some edge cases
                                          --accounted for here where the change in tiers, prices, and ARR do not all happen in the same month.

    SELECT
      arr_month,
      sfdc_account_id,
      CASE
        WHEN previous_month_product_category = product_category
          THEN quantity * (arr/NULLIF(quantity,0) - previous_month_arr/NULLIF(previous_month_quantity,0))
        WHEN previous_month_product_category != product_category AND previous_month_product_ranking = product_ranking
          THEN quantity * (arr/NULLIF(quantity,0) - previous_month_arr/NULLIF(previous_month_quantity,0))
        ELSE 0
      END                     AS price_change_arr
    FROM type_of_arr_change

), reason_for_arr_change_tier_change AS (--Calculate the change in ARR attributable to a change in product tier. From Bronze to Silver for example.

    SELECT
      arr_month,
      sfdc_account_id,
      CASE
        WHEN previous_month_product_ranking != product_ranking
        THEN ZEROIFNULL(quantity * (arr/NULLIF(quantity,0) - previous_month_arr/NULLIF(previous_month_quantity,0)))
        ELSE 0
      END                     AS tier_change_arr
    FROM type_of_arr_change

), reason_for_arr_change_end  AS (--Create a record for the ending ARR for each account by month

    SELECT
      arr_month,
      sfdc_account_id,
      arr                     AS end_arr,
      quantity                AS end_quantity
    FROM type_of_arr_change

), annual_price_per_seat_change AS (--Calculate the change in price per seat

  SELECT
    arr_month,
    sfdc_account_id,
    ZEROIFNULL(( arr / NULLIF(quantity,0) ) - ( previous_month_arr / NULLIF(previous_month_quantity,0))) AS annual_price_per_seat_change
  FROM type_of_arr_change

), account_info AS (--Add additional customer account attributes to the model

    SELECT DISTINCT
      type_of_arr_change.arr_month,
      type_of_arr_change.sfdc_account_id,
      CASE
        WHEN sfdc_accounts_xf.account_segment = 'Unknown'
        THEN 'SMB'
        ELSE sfdc_accounts_xf.account_segment
      END                                        AS account_segment,
   	  CASE
        WHEN sfdc_accounts_xf.df_industry IS NULL
        THEN 'Unknown'
        ELSE sfdc_accounts_xf.df_industry
      END                                        AS account_industry,
      CASE
        WHEN sfdc_accounts_xf.tsp_account_employees IS NULL
        THEN '0'
        ELSE sfdc_accounts_xf.tsp_account_employees
      END                                        AS account_employee_count
    FROM type_of_arr_change
    LEFT JOIN sfdc_accounts_xf
      ON type_of_arr_change.sfdc_account_id = sfdc_accounts_xf.account_id
    ORDER BY 1,4,5

), final AS (

    SELECT
      {{ dbt_utils.surrogate_key(['type_of_arr_change.arr_month', 'type_of_arr_change.sfdc_account_id']) }} AS primary_key,
      type_of_arr_change.arr_month,
      type_of_arr_change.months_since_sfdc_account_cohort_start,
      type_of_arr_change.quarters_since_sfdc_account_cohort_start,
      type_of_arr_change.ultimate_parent_account_name,
      type_of_arr_change.sfdc_account_name,
      type_of_arr_change.sfdc_account_id,
      account_info.account_segment,
      account_info.account_industry,
      account_info.account_employee_count,
      type_of_arr_change.product_category,
      type_of_arr_change.previous_month_product_category,
      type_of_arr_change.delivery,
      type_of_arr_change.previous_month_delivery,
      type_of_arr_change.product_ranking,
      type_of_arr_change.previous_month_product_ranking,
      type_of_arr_change.type_of_arr_change,
      reason_for_arr_change_beg.beg_arr,
      reason_for_arr_change_beg.beg_quantity,
      reason_for_arr_change_seat_change.seat_change_arr,
      reason_for_arr_change_seat_change.seat_change_quantity,
      reason_for_arr_change_price_change.price_change_arr,
      reason_for_arr_change_tier_change.tier_change_arr,
      reason_for_arr_change_end.end_arr,
      reason_for_arr_change_end.end_quantity,
      annual_price_per_seat_change.annual_price_per_seat_change
    FROM type_of_arr_change
    LEFT JOIN  account_info
      ON type_of_arr_change.sfdc_account_id = account_info.sfdc_account_id
      AND type_of_arr_change.arr_month = account_info.arr_month
    LEFT JOIN reason_for_arr_change_beg
      ON type_of_arr_change.sfdc_account_id = reason_for_arr_change_beg.sfdc_account_id
      AND type_of_arr_change.arr_month = reason_for_arr_change_beg.arr_month
    LEFT JOIN reason_for_arr_change_seat_change
      ON type_of_arr_change.sfdc_account_id = reason_for_arr_change_seat_change.sfdc_account_id
      AND type_of_arr_change.arr_month = reason_for_arr_change_seat_change.arr_month
    LEFT JOIN reason_for_arr_change_price_change
      ON type_of_arr_change.sfdc_account_id = reason_for_arr_change_price_change.sfdc_account_id
      AND type_of_arr_change.arr_month = reason_for_arr_change_price_change.arr_month
    LEFT JOIN reason_for_arr_change_tier_change
      ON type_of_arr_change.sfdc_account_id = reason_for_arr_change_tier_change.sfdc_account_id
      AND type_of_arr_change.arr_month = reason_for_arr_change_tier_change.arr_month
    LEFT JOIN reason_for_arr_change_end
      ON type_of_arr_change.sfdc_account_id = reason_for_arr_change_end.sfdc_account_id
      AND type_of_arr_change.arr_month = reason_for_arr_change_end.arr_month
    LEFT JOIN annual_price_per_seat_change
      ON type_of_arr_change.sfdc_account_id = annual_price_per_seat_change.sfdc_account_id
      AND type_of_arr_change.arr_month = annual_price_per_seat_change.arr_month

)

SELECT *
FROM final
WHERE final.arr_month < DATE_TRUNC('month', CURRENT_DATE)
