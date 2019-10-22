WITH licenses AS (
  SELECT *
  FROM {{ ref('license_db_licenses') }}
  WHERE license_md5 IS NOT NULL
    AND is_trial = False
),

usage_data AS (
  SELECT *
  FROM {{ ref('pings_usage_data') }}
  WHERE license_md5 IS NOT NULL
),

week_spine AS (
  SELECT DISTINCT
    first_day_of_week AS week
  FROM {{ ref('date_details') }}
  WHERE date_details.first_day_of_week  BETWEEN '2017-04-01' AND CURRENT_DATE
),

grouped AS (
  SELECT
    week,
    licenses.license_id,
    licenses.license_md5,
    MAX(IFF(usage_data.id IS NOT NULL, 1, 0)) AS did_send_usage_data,
    COUNT(*)                                  AS count_usage_data_pings,
    MIN(usage_data.created_at)                AS min_usage_data_created_at,
    MAX(usage_data.created_at)                AS max_usage_data_created_at
  FROM week_spine
    LEFT JOIN licenses
      ON week_spine.week BETWEEN licenses.starts_at AND COALESCE(licenses.license_expires_at, '9999-12-31')
    LEFT JOIN usage_data
      ON licenses.license_md5 = usage_data.license_md5
  GROUP BY 1,2,3
)

SELECT
  week,
  license_id,
  license_md5,
  did_send_usage_data::BOOLEAN AS did_send_usage_data,
  count_usage_data_pings,
  min_usage_data_created_at,
  max_usage_data_created_at
FROM grouped
