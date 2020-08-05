WITH source AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'cluster_groups') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1
  
)

, renamed AS (
  
    SELECT
    
      id::NUMBER           AS cluster_group_id,
      cluster_id::NUMBER   AS cluster_id,
      group_id::NUMBER     AS group_id

    FROM source
  
)

SELECT * 
FROM renamed
