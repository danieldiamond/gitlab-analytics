WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'procurement_cost_savings') }}

), renamed AS (
  
    SELECT
      calendar_month::DATE                                                                          AS calendar_month,
      TRY_TO_DECIMAL(REPLACE(REPLACE(savings,'$'),','), 14, 2)                                      AS savings,
      TRY_TO_DECIMAL(REPLACE(REPLACE(rolling_12_month_savings_without_audit,'$'),','), 14, 2)       AS rolling_12_month_savings_without_audit,
      TRY_TO_DECIMAL(REPLACE(REPLACE(rolling_12_month_savings_with_audit,'$'),','), 14, 2)          AS rolling_12_month_savings_with_audit,
      TRY_TO_DECIMAL(REPLACE(REPLACE(target,'$'),','), 14, 2)                                       AS target
    FROM source

)

SELECT *
FROM renamed
