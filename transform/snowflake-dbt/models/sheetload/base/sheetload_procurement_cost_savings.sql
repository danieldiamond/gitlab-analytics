WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'procurement_cost_savings') }}

), renamed AS (

    SELECT
      TRY_TO_DATE(calendar_month::VARCHAR)                                      AS calendar_month,
      TRY_TO_DECIMAL(savings::VARCHAR,14,2)                                     AS savings,
      TRY_TO_DECIMAL(rolling_12_month_savings_without_audit::VARCHAR,14,2)      AS rolling_12_month_savings_without_audit,
      TRY_TO_DECIMAL(rolling_12_month_savings_with_audit::VARCHAR,14,2)         AS rolling_12_month_savings_with_audit,
      TRY_TO_DECIMAL(target::VARCHAR,14,2)                                      AS target
    FROM source

)

SELECT *
FROM renamed

