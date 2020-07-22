--Test that the beg ARR, plus the changes that are calculated independently, equals the ending ARR

WITH test AS (

    SELECT
      arr_month,
      sfdc_account_id,
      SUM(beg_arr + seat_change_arr + price_change_arr + tier_change_arr)       AS beg_plus_changes_arr,
      SUM(end_arr)                                                              AS end_arr,
      SUM(beg_quantity + seat_change_quantity)                                  AS beg_plus_changes_quantity,
      SUM(end_quantity)                                                         AS end_quantity
    FROM {{ ref('arr_changes_monthly') }}
    GROUP BY 1,2

), variance AS(

    SELECT
      test.arr_month,
      test.sfdc_account_id,
      ROUND((test.beg_plus_changes_arr - test.end_arr))                         AS arr_variance,
      test.beg_plus_changes_quantity - test.end_quantity                        AS quanity_variance
    FROM test

)

SELECT *
FROM variance
WHERE arr_variance != 0
  OR quanity_variance !=0
