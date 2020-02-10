{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rank_in_key
  FROM {{ source('license', 'licenses') }}

), renamed AS (

  SELECT DISTINCT
    id::INTEGER                                  AS license_id,
    company::VARCHAR                             AS company,
    users_count::INTEGER                         AS users_count,
    email::VARCHAR                               AS email,
    md5(license_file::VARCHAR)                   AS license_md5,
    expires_at::TIMESTAMP                        AS license_expires_at,
    plan_name::VARCHAR                           AS plan_name,
    starts_at::TIMESTAMP                         AS starts_at,
    NULLIF(zuora_subscription_id, '')::VARCHAR   AS zuora_subscription_id,
    previous_users_count::INTEGER                AS previous_users_count,
    trueup_quantity::INTEGER                     AS trueup_quantity,
    trueup_from::TIMESTAMP                       AS trueup_from,
    trueup_to::TIMESTAMP                         AS trueup_to,
    plan_code::VARCHAR                           AS plan_code,
    trial::BOOLEAN                               AS is_trial,
    created_at::TIMESTAMP                        AS created_at,
    updated_at::TIMESTAMP                        AS updated_at
 FROM source
 WHERE rank_in_key = 1

)

SELECT *
FROM renamed
