WITH source AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'cluster_projects') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1
  
)

, renamed AS (
  
    SELECT
      id::INTEGER           AS cluster_project_id,
      cluster_id::INTEGER   AS cluster_id,
      project_id::INTEGER   AS project_id
      
    FROM source
  
)

SELECT * 
FROM renamed
