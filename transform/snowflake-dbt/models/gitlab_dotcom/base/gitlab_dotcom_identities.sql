{{ config({
    "schema": "sensitive"
    })
}}

WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rank_in_key
  FROM {{ source('gitlab_dotcom', 'identities') }}

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
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed
