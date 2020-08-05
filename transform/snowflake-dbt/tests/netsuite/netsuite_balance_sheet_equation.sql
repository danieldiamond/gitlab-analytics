--Tests to see if assets = liabilities + owner's equity

WITH balance_sheet AS (

    SELECT *
    FROM {{ ref('netsuite_actuals_balance_sheet') }}

), equation AS (

    SELECT
      accounting_period,
      SUM(CASE
            WHEN balance_sheet_grouping_level_3 = '1-assets'
              THEN actual_amount
          END)                                          AS total_assets,
      SUM(CASE
            WHEN balance_sheet_grouping_level_3 = '2-liabilities & equity'
              THEN actual_amount
          END)                                          AS total_liabilities_equity,
      (total_assets - total_liabilities_equity)::INT    AS balance_sheet_equation
    FROM balance_sheet
    GROUP BY 1
    ORDER BY 1

)

SELECT *
FROM equation
WHERE balance_sheet_equation != 0
