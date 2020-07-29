WITH zuora_subscription AS (
  SELECT *
  FROM {{ ref('zuora_subscription_source') }}
),
dim_dates AS (
  SELECT *
  FROM {{ ref('dim_dates') }}
),
created_date AS (
  SELECT
    CAST(created_date AS DATE) created_date,
    COUNT(*) AS num_rows
  FROM zuora_subscription
  GROUP BY CAST(created_date AS DATE)
),
updated_date AS (
  SELECT
    CAST(updated_date AS DATE) updated_date,
    COUNT(*)AS num_rows
  FROM zuora_subscription
  GROUP BY CAST(updated_date AS DATE)
)

SELECT
    DISTINCT
      db.date_day            AS date_day,
      db.day_name            AS day_name
      ISNULL(cd.num_rows, 0) AS created_subscriptions,
      ISNULL(ud.num_rows, 0) AS updated_subscriptions
FROM dim_dates db
LEFT JOIN created_date cd ON cd.created_date = db.date_day
LEFT JOIN updated_date ud ON ud.updated_date = db.date_day
WHERE (cd.num_rows > 0 OR ud.num_rows > 0)