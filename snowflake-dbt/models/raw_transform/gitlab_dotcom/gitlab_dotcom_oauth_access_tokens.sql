WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'oauth_access_tokens') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1

), renamed AS (

    SELECT
      id::INTEGER                 AS oauth_access_token_id,
      resource_owner_id::INTEGER  AS resource_owner_id,
      application_id::INTEGER     AS application_id,
      expires_in::INTEGER         AS expires_in_seconds,
      revoked_at::TIMESTAMP       AS oauth_access_token_revoked_at,
      created_at::TIMESTAMP       AS created_at,
      scopes::VARCHAR             AS scopes
    FROM source

)

SELECT *
FROM renamed
ORDER BY created_at
