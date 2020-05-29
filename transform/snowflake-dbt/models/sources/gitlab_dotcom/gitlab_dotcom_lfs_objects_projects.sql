WITH source AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'lfs_objects_projects') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1
  
)

, renamed AS (
  
    SELECT 
      id::INTEGER              AS lfs_object_project_id,
      lfs_object_id::INTEGER   AS lfs_object_id,
      project_id::INTEGER      AS project_id,
      created_at::TIMESTAMP    AS created_at,
      updated_at::TIMESTAMP    AS updated_at,
      repository_type::VARCHAR AS repository_type
      
    FROM source
  
)

SELECT * 
FROM renamed
