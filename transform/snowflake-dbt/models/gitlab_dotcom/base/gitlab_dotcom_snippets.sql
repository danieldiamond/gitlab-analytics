{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'snippets') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1

), renamed AS (
  
    SELECT
        id::INTEGER               AS snippet_id,
        title::VARCHAR            AS snippet_title,
        author_id::INTEGER        AS author_id,
        project_id::INTEGER       AS project_id,
        created_at::TIMESTAMP     AS created_at,
        updated_at::TIMESTAMP     AS updated_at,
        type::VARCHAR             AS type,
        visibility_level::VARCHAR AS visibility_level
    FROM source
    
)

SELECT * 
FROM renamed
