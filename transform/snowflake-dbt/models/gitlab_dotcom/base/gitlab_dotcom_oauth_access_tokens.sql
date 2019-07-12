{{ config({
    "schema": "analytics",
    "post-hook": "grant select on {{this}} to role reporter"
    })
}}

WITH source AS (

  SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) as rank_in_key
  FROM {{ source('gitlab_dotcom', 'oauth_access_tokens') }}

), renamed AS (

    SELECT
      id::integer                 AS oauth_access_token_id,
      resource_owner_id::integer  AS resource_owner_id,
      application_id::integer     AS application_id,
      token::varchar              AS oauth_access_token,
      refresh_token::varchar      AS oauth_access_refresh_token,
      expires_in::integer         AS expires_in_seconds,
      revoked_at::timestamp       AS oauth_access_token_revoked_at,
      created_at::timestamp       AS oauth_access_token_created_at,
      scopes::varchar             AS scopes
    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed
ORDER BY oauth_access_token_created_at
