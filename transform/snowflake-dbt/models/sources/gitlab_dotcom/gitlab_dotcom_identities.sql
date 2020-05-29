{{ config({
    "schema": "sensitive"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'identities') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

),
renamed AS (

    SELECT
      id::INTEGER                 AS identity_id,
      extern_uid::VARCHAR         AS extern_uid,
      provider::VARCHAR           AS identity_provider,
      user_id::INTEGER            AS user_id,
      created_at::TIMESTAMP       AS created_at,
      updated_at::TIMESTAMP       AS updated_at,
      --econdary_extern_uid // always null
      saml_provider_id::INTEGER   AS saml_provider_id
    FROM source

)

SELECT *
FROM renamed
