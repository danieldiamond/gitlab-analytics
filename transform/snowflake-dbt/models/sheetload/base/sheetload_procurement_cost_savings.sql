WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'procurement_cost_savings') }}

), renamed AS (

    SELECT
      calendar_month::VARCHAR                              AS calendar_month,
      savings::VARCHAR                                     AS savings,
      rolling_12_month_savings_without_audit::VARCHAR      AS rolling_12_month_savings_without_audit,
      rolling_12_month_savings_with_audit::VARCHAR         AS rolling_12_month_savings_with_audit,
      target::VARCHAR                                      AS target
    FROM source

)

SELECT *
FROM renamed

