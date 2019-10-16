{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) as rank_in_key
  FROM {{ source('gitlab_dotcom', 'ci_trigger_requests') }}

), renamed AS (
  
  SELECT

    id::INTEGER           AS ci_trigger_request_id,
    trigger_id::INTEGER   AS trigger_id,
    created_at::TIMESTAMP AS created_at,
    updated_at::TIMESTAMP AS updated_at,
    commit_id::INTEGER    AS commit_id
    
  FROM source
  WHERE rank_in_key = 1
  
)

SELECT * 
FROM renamed
