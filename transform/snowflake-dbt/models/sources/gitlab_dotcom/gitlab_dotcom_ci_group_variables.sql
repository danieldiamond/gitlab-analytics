WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'ci_group_variables') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

  SELECT 
    id::NUMBER             AS ci_group_variable_id, 
    key                     AS key, 
    group_id::NUMBER       AS ci_group_variable_group_id, 
    created_at::TIMESTAMP   AS created_at, 
    updated_at::TIMESTAMP   AS updated_at, 
    masked                  AS masked, 
    variable_type           AS variable_variable_type 
  FROM source

)


SELECT *
FROM renamed
