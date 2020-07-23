--Test that the beg ARR, plus the changes that are calculated independently, equals the ending ARR. Also check to see that
--the end arr ties out to the arr in mrr_totals_levelled.

WITH test AS (

    SELECT
      arr_month,
      sfdc_account_id,
      SUM(beg_arr + seat_change_arr + price_change_arr + tier_change_arr)       AS beg_plus_changes_arr,
      SUM(end_arr)                                                              AS end_arr,
      SUM(beg_quantity + seat_change_quantity)                                  AS beg_plus_changes_quantity,
      SUM(end_quantity)                                                         AS end_quantity
    FROM {{ ref('arr_changes_sfdc_account_monthly') }}
    GROUP BY 1,2

), mrr_totals_levelled AS(

    SELECT
      mrr_month          AS arr_month,
      sfdc_account_id,
      SUM(mrr*12)        AS arr,
      SUM(quantity)      AS quantity
    FROM {{ ref('mrr_totals_levelled') }}
    GROUP BY 1,2

), variance AS(

    SELECT
      test.arr_month,
      test.sfdc_account_id,
      ROUND((test.beg_plus_changes_arr - test.end_arr))                         AS arr_variance,
      test.beg_plus_changes_quantity - test.end_quantity                        AS quanity_variance,
      ROUND(test.end_arr - mrr_totals_levelled.arr)                             AS mrr_totals_arr_variance,
      test.end_quantity - mrr_totals_levelled.quantity                          AS mrr_totals_quantity_variance
    FROM test
    LEFT JOIN mrr_totals_levelled
      ON test.arr_month = mrr_totals_levelled.arr_month
      AND test.sfdc_account_id = mrr_totals_levelled.sfdc_account_id

)

SELECT *
FROM variance
WHERE arr_variance != 0
  OR quanity_variance !=0
  OR mrr_totals_arr_variance != 0
  OR mrr_totals_quantity_variance != 0
