WITH licenses AS (

    SELECT *
    FROM {{ ref('license_db_licenses_source') }}

)

SELECT
  license_id,
  license_md5,
  zuora_subscription_id,
  zuora_subscription_name,
  users_count              AS license_user_count,
  company,
  plan_code,
  is_trial,
  starts_at::DATE          AS license_start_date,
  license_expires_at::DATE AS license_expire_date,
  created_at,
  updated_at
FROM licenses
