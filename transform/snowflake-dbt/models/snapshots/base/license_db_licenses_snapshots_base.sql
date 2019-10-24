{{ config({
    "schema": "staging",
    "alias": "license_db_licenses_snapshots"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ source('snapshots', 'license_db_licenses_snapshots') }}

), renamed AS (

  SELECT DISTINCT
    dbt_scd_id::VARCHAR                AS license_snapshot_id,
    id::INTEGER                        AS license_id,
    company::VARCHAR                   AS company,
    users_count::INTEGER               AS users_count,
    email::VARCHAR                     AS email,
    license_md5::VARCHAR               AS license_md5,
    expires_at::TIMESTAMP              AS license_expires_at,
    recurly_subscription_id::VARCHAR   AS recurly_subscription_id,
    plan_name::VARCHAR                 AS plan_name,
    starts_at::TIMESTAMP               AS starts_at,
    zuora_subscription_id::VARCHAR     AS zuora_subscription_id,
    previous_users_count::INTEGER      AS previous_users_count,
    trueup_quantity::INTEGER           AS trueup_quantity,
    trueup_from::TIMESTAMP             AS trueup_from,
    trueup_to::TIMESTAMP               AS trueup_to,
    plan_code::VARCHAR                 AS plan_code,
    trial::BOOLEAN                     AS is_trial,
    created_at::TIMESTAMP              AS created_at,
    updated_at::TIMESTAMP              AS updated_at,
    "DBT_VALID_FROM"::TIMESTAMP        AS valid_from,
    "DBT_VALID_TO"::TIMESTAMP          AS valid_to
 FROM source

)

SELECT *
FROM renamed
