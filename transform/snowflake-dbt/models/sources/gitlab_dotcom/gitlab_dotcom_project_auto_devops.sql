WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'project_auto_devops') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

    SELECT
      id::NUMBER                      AS project_auto_devops_id,
      project_id::NUMBER              AS project_id,
      created_at::TIMESTAMP            AS created_at,
      updated_at::TIMESTAMP            AS updated_at,
      enabled::BOOLEAN                 AS has_auto_devops_enabled

    FROM source
)

SELECT *
FROM renamed
