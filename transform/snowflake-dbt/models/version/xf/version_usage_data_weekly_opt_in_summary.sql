{{ config({
    "materialized": "table"
    })
}}


WITH usage_data AS (

  SELECT *
  FROM {{ ref('version_usage_data_unpacked') }}
  WHERE license_md5 IS NOT NULL

)

, week_spine AS (

  SELECT DISTINCT
    DATE_TRUNC('week', date_actual) AS week
  FROM {{ ref('date_details') }}
  WHERE date_details.date_actual BETWEEN '2017-04-01' AND CURRENT_DATE

)

, grouped AS (

  SELECT
    {{ dbt_utils.surrogate_key('week', 'licenses.license_id') }} AS week_license_unique_id,
    week_spine.week,
    licenses.license_id,
    licenses.license_md5,
    licenses.zuora_subscription_id,
    licenses.plan_code                                           AS product_category,
    MAX(IFF(usage_data.id IS NOT NULL, 1, 0))                    AS did_send_usage_data,
    COUNT(DISTINCT usage_data.id)                                AS count_usage_data_pings,
    MIN(usage_data.created_at)                                   AS min_usage_data_created_at,
    MAX(usage_data.created_at)                                   AS max_usage_data_created_at
  FROM week_spine
    LEFT JOIN licenses
      ON week_spine.week BETWEEN licenses.starts_at AND {{ coalesce_to_infinity("licenses.license_expires_at") }}
    LEFT JOIN usage_data
      ON licenses.license_md5 = usage_data.license_md5
      AND week_spine.week = DATE_TRUNC('week', usage_data.created_at)
  {{ dbt_utils.group_by(n=6) }}

)

, alphabetized AS (

    SELECT
      week_license_unique_id,

      week,
      license_id,

      license_md5,
      product_category,
      zuora_subscription_id,

      --metadata
      count_usage_data_pings,
      did_send_usage_data::BOOLEAN AS did_send_usage_data,
      min_usage_data_created_at,
      max_usage_data_created_at
    FROM grouped

)

SELECT *
FROM alphabetized
