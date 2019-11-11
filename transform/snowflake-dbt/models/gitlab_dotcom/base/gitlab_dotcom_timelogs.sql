{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT
    *
  FROM {{ source('gitlab_dotcom', 'timelogs') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1


), renamed AS (
  
    SELECT 
      id AS timelog_id,
      created_at,
      spent_at,
      updated_at,
      
      time_spent AS second_spent,
      
      issue_id,
      merge_request_id,
      user_id
    FROM source
    
)

SELECT * 
FROM renamed
