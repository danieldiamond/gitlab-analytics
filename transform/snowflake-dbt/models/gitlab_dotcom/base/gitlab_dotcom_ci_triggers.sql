{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY id ORDER BY UPDATED_AT DESC) as rank_in_key
  FROM {{ source('gitlab_dotcom', 'ci_triggers') }}

), renamed AS (
  
  SELECT
  
    id::INTEGER           AS ci_trigger_id,
    token                 AS token,
    created_at::TIMESTAMP AS ci_trigger_created_at,
    updated_at::TIMESTAMP AS ci_trigger_updated_at,
    project_id::INTEGER   AS ci_trigger_project_id,
    owner_id::INTEGER     AS ci_trigger_owner_id,
    description           AS description
    
  FROM source 
  
)

SELECT * 
FROM renamed
