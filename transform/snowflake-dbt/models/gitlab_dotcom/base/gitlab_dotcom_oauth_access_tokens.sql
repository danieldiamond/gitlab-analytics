WITH source AS (

  SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) AS rank_in_key
  FROM {{ source('gitlab_dotcom', 'oauth_access_tokens') }}

), renamed AS (

    SELECT
      id::INTEGER                 AS oauth_access_token_id,
      resource_owner_id::INTEGER  AS resource_owner_id,
      application_id::INTEGER     AS application_id,
      refresh_token::VARCHAR      AS oauth_access_refresh_token,
      expires_in::INTEGER         AS expires_in_seconds,
      revoked_at::TIMESTAMP       AS oauth_access_token_revoked_at,
      created_at::TIMESTAMP       AS oauth_access_token_created_at,
      scopes::VARCHAR             AS scopes
    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed
ORDER BY oauth_access_token_created_at
