WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) as rank_in_key
  FROM {{ source('version', 'version_checks') }}

), renamed AS (

  SELECT
    id::INTEGER                AS id,
    host_id::INTEGER           AS host_id,
    created_at::TIMESTAMP      AS created_at,
    updated_at::TIMESTAMP      AS updated_at,
    gitlab_version::VARCHAR    AS gitlab_version,
    referer_url::VARCHAR       AS referer_url,
    PARSE_JSON(request_data)   AS request_data
  FROM source
  WHERE rank_in_key = 1
)

SELECT * 
FROM renamed
