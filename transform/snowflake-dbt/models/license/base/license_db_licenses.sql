{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY UPDATED_AT DESC) AS rank_in_key
  FROM {{ source('license', 'license_db_licenses') }}

), renamed AS (

  SELECT DISTINCT
    id::INTEGER                   AS license_id,
    company::VARCHAR              AS company,
    users_count::INTEGER          AS,
    expires_at,
    recurly_subscription_id,
    plan_name,
    starts_at,
    zuora_subscription_id,
    previous_users_count,
    trueup_quantity,
    trueup_from,
    trueup_to,
    plan_code,
    trial,
    created_at,
    updated_at

 FROM source
 WHERE rank_in_key = 1
)

SELECT *
FROM renamed
