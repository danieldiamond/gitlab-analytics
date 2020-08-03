WITH licenses AS (

    SELECT *
    FROM {{ ref('license_db_licenses_source') }}

), renamed AS (

    SELECT
      license_id,
      license_md5,
      zuora_subscription_id,
      zuora_subscription_name,
      users_count              AS license_user_count,
      company,
      plan_code,
      is_trial,
      IFF(
          LOWER(email) LIKE '%@gitlab.com' AND LOWER(company) LIKE '%gitlab%',
          TRUE, FALSE
      )                        AS is_internal,
      starts_at::DATE          AS license_start_date,
      license_expires_at::DATE AS license_expire_date,
      created_at,
      updated_at
    FROM licenses

)

SELECT *
FROM renamed