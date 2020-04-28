WITH source AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'web_hooks') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
)

SELECT 
  id::INTEGER                 AS web_hook_id,
  project_id::VARCHAR         AS project_id,
  type::VARCHAR               AS web_hook_type,
  service_id::INTEGER         AS service_id,
  created_at::TIMESTAMP       AS created_at,
  updated_at::TIMESTAMP       AS updated_at
FROM source