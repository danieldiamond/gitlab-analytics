WITH licenses AS (

    SELECT *
    FROM {{ ref('license_db_licenses_source') }}

), renamed AS (

    SELECT
      license_id,
      license_md5,
      zuora_subscription_id    AS subscription_id,
      zuora_subscription_name  AS subscription_name,
      users_count              AS license_user_count,
      plan_code                AS license_plan,
      is_trial,
      IFF(
          LOWER(email) LIKE '%@gitlab.com' AND LOWER(company) LIKE '%gitlab%',
          TRUE, FALSE
      )                        AS is_internal,
      company,
      starts_at::DATE          AS license_start_date,
      license_expires_at::DATE AS license_expire_date,
      created_at,
      updated_at
    FROM licenses

)

SELECT *
FROM renamed