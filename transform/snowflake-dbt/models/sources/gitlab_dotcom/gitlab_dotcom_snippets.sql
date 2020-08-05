WITH source AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'snippets') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1

), renamed AS (
  
    SELECT
      id::NUMBER               AS snippet_id,
      author_id::NUMBER        AS author_id,
      project_id::NUMBER       AS project_id,
      created_at::TIMESTAMP     AS created_at,
      updated_at::TIMESTAMP     AS updated_at,
      type::VARCHAR             AS snippet_type,
      visibility_level::NUMBER AS visibility_level
      
    FROM source
    
)

SELECT * 
FROM renamed
