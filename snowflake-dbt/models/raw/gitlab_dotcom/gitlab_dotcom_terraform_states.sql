WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'terraform_states') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

    SELECT

      id::INTEGER               AS terraform_state_id,
      project_id::INTEGER       AS project_id,
      created_at::TIMESTAMP     AS created_at,
      updated_at::TIMESTAMP     AS updated_at,
      file_store::VARCHAR       AS file_store

    FROM source

)

SELECT  *
FROM renamed
