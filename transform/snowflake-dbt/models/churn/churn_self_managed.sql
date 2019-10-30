WITH licenses AS (

    SELECT *
    FROM {{ref('license_db_licenses')}}

), zuora_subs AS (

    SELECT *
    FROM {{ref('zuora_subscription')}}

)


SELECT
  *
FROM licenses
  LEFT JOIN zuora_subs
    ON licenses.zuora_subscription_id = zuora_subs.subscription_id
WHERE licenses.license_expires_at < CURRENT_DATE
  AND licenses.zuora_subscription_id IS NOT NULL
