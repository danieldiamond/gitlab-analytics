WITH zuora_account AS (

  SELECT *
  FROM {{ ref('zuora_account_source') }}

), dim_dates AS (

  SELECT *
  FROM {{ ref('dim_dates') }}

), created_date AS (

  SELECT
    CAST(created_date AS DATE)          AS created_date,
    COUNT(*)                            AS num_rows
  FROM zuora_account
  GROUP BY CAST(created_date AS DATE)

), updated_date AS (

  SELECT
    CAST(updated_date AS DATE)          AS updated_date,
    COUNT(*)                            AS num_rows
  FROM zuora_account
  GROUP BY CAST(updated_date AS DATE)

)

SELECT
    DISTINCT
      db.date_day                       AS date_day,
      db.day_name                       AS day_name,
      IFNULL(cd.num_rows, 0)            AS created_accounts,
      IFNULL(ud.num_rows, 0)            AS updated_accounts
FROM dim_dates db
LEFT JOIN created_date cd
  ON cd.created_date = db.date_day
LEFT JOIN updated_date ud
  ON ud.updated_date = db.date_day
WHERE (cd.num_rows > 0 OR ud.num_rows > 0)
