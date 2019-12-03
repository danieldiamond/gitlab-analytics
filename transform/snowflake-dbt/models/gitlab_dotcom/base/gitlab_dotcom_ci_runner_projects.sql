{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'ci_runner_projects') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
  
)

, renamed AS (
  
    SELECT
    
      id::INTEGER           AS ci_runner_project_id,
      runner_id::INTEGER    AS runner_id,
      project_id::INTEGER   AS project_id,
      created_at::TIMESTAMP AS created_at,
      updated_at::TIMESTAMP AS updated_at

    FROM source
    WHERE porject_id IS NOT NULL
  
)

SELECT * 
FROM renamed
