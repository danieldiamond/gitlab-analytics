{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY project_id ORDER BY updated_at DESC) AS rank_in_key
  FROM {{ source('gitlab_dotcom', 'project_auto_devops') }}

), renamed AS (

    SELECT

      project_id::INTEGER              AS project_id,
      created_at::TIMESTAMP            AS project_auto_devops_created_at,
      updated_at::TIMESTAMP            AS project_auto_devops_updated_at,
      enabled::BOOLEAN                 AS has_auto_devops_enabled

    FROM source
    WHERE rank_in_key = 1
)

SELECT *
FROM renamed
