WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'timelogs') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1


), renamed AS (
  
    SELECT 
      id::INTEGER               AS timelog_id,
      created_at::TIMESTAMP     AS created_at,
      spent_at::TIMESTAMP       AS spent_at,
      updated_at::TIMESTAMP     AS updated_at,
      
      time_spent::INTEGER       AS seconds_spent,
      
      issue_id::INTEGER         AS issue_id,
      merge_request_id::INTEGER AS merge_request_id,
      user_id::INTEGER          AS user_id  
    FROM source
    
)

SELECT * 
FROM renamed
