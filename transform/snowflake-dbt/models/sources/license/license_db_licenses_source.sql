WITH source AS (

    SELECT *
    FROM {{ source('license', 'licenses') }}

), renamed AS (

    SELECT
      id::NUMBER                                   AS license_id,
      company::VARCHAR                             AS company,
      users_count::NUMBER                          AS users_count,
      email::VARCHAR                               AS email,
      md5(license_file::VARCHAR)                   AS license_md5,
      expires_at::TIMESTAMP                        AS license_expires_at,
      plan_name::VARCHAR                           AS plan_name,
      starts_at::TIMESTAMP                         AS starts_at,
      NULLIF(zuora_subscription_name, '')::VARCHAR AS zuora_subscription_name,
      NULLIF(zuora_subscription_id, '')::VARCHAR   AS zuora_subscription_id,
      previous_users_count::NUMBER                 AS previous_users_count,
      trueup_quantity::NUMBER                      AS trueup_quantity,
      trueup_from::TIMESTAMP                       AS trueup_from,
      trueup_to::TIMESTAMP                         AS trueup_to,
      plan_code::VARCHAR                           AS plan_code,
      trial::BOOLEAN                               AS is_trial,
      created_at::TIMESTAMP                        AS created_at,
      updated_at::TIMESTAMP                        AS updated_at
    FROM source

)

SELECT *
FROM renamed
