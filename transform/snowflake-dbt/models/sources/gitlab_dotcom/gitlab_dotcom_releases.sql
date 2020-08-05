WITH source AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'releases') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (
  
    SELECT
      id::NUMBER           AS release_id,
      tag::VARCHAR          AS tag,
      project_id::VARCHAR   AS project_id,
      created_at::TIMESTAMP AS created_at,
      updated_at::TIMESTAMP AS updated_at,
      author_id::NUMBER    AS author_id
    FROM source
    
)

SELECT * 
FROM renamed
