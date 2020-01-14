WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'rep_productivity') }}

), renamed AS (

    SELECT
      sales_rep::VARCHAR    AS sales_rep,
      month::DATE           AS calendar_month,
      rep_role::VARCHAR     AS rep_role,
      percent_ramped::FLOAT AS percent_ramped
    FROM source

)

SELECT *
FROM renamed