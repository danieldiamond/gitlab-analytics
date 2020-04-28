WITH source AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'web_hook_logs') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
)

SELECT 
  id::INTEGER                 As web_hook_log_id,
  web_hook_id::INTEGER        AS web_hook_id,
  url::VARCHAR                AS web_hook_url,
  response_status::VARCHAR    AS response_status,
  execution_duration::NUMERIC AS execution_duration,
  created_at::TIMESTAMP       AS created_at,
  updated_at::TIMESTAMP       AS updated_at
FROM source