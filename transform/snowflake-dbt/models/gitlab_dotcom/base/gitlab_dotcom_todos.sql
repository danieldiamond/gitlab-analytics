{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'todos') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1

), renamed AS (
  
    SELECT
    
      id::INTEGER           AS id,
      user_id::INTEGER      AS user_id,
      project_id::INTEGER   AS project_id,
      target_id::INTEGER    AS target_id,
      target_type::VARCHAR  AS target_type,
      author_id::INTEGER    AS author_id,
      action::VARCHAR       AS todo_action,
      state::VARCHAR        AS todo_state,
      created_at::TIMESTAMP AS created_at,
      updated_at::TIMESTAMP AS updated_at,
      note_id::INTEGER      AS note_id,
      commit_id::VARCHAR    AS commit_id,
      group_id::INTEGER     AS group_id
      
    FROM source
    
)

SELECT * 
FROM renamed
