WITH golden_records AS (
      SELECT *
      FROM {{ ref('sheetload_account_golden_records_source') }}
  )


SELECT *
FROM golden_records