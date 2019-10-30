WITH licenses AS (

    SELECT *
    FROM {{ref('license_db_licenses')}}

), zuora_subs AS (

    SELECT *
    FROM {{ref('zuora_subscription')}}

)


SELECT
  licenses.license_id,
  licenses.company,
  licenses.users_count,
  licenses.license_md5,
  licenses.starts_at,
  licenses.license_expires_at,

  zuora_subs.subscription_id,
  zuora_subs.subscription_name_slugify,
  
FROM licenses
  LEFT JOIN zuora_subs
    ON licenses.zuora_subscription_id = zuora_subs.subscription_id
WHERE licenses.license_expires_at < CURRENT_DATE
  AND licenses.zuora_subscription_id IS NOT NULL
