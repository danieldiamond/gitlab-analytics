{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'ci_triggers') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (
  
  SELECT
  
    id::INTEGER           AS ci_trigger_id,
    token::VARCHAR        AS token,
    created_at::TIMESTAMP AS created_at,
    updated_at::TIMESTAMP AS updated_at,
    project_id::INTEGER   AS project_id,
    owner_id::INTEGER     AS owner_id,
    description::VARCHAR  AS ci_trigger_description
    
  FROM source
  
)

SELECT * 
FROM renamed
