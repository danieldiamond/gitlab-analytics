WITH source AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'repository_languages') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1

), renamed AS (
  
    SELECT
      id::INTEGER           AS release_id,
      tag::VARCHAR          AS tag,
      project_id::VARCHAR   AS project_id,
      created_at::TIMESTAMP AS created_at,
      updated_at::TIMESTAMP AS updated_at,
      author_id::INTEGER    AS author_id
    FROM source
    
)

SELECT * 
FROM renamed
