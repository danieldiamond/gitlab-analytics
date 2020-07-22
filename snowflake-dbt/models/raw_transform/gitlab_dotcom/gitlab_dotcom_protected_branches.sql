WITH source AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'protected_branches') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (
  
    SELECT
      id::INTEGER                           AS protected_branch_id,
      name::VARCHAR                         AS protected_branch_name,
      project_id::VARCHAR                   AS project_id,
      created_at::TIMESTAMP                 AS created_at,
      updated_at::TIMESTAMP                 AS updated_at,
      code_owner_approval_required::BOOLEAN AS is_code_owner_approval_required
    FROM source
    
)

SELECT * 
FROM renamed
