WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'system_note_metadata') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

    SELECT
      id::INTEGER                           AS system_note_metadata,
      note_id::INTEGER                      AS note_id,
      commit_count::INTEGER                 AS commit_count,
      action::VARCHAR                       AS action_type,
      description_version_id::INTEGER       AS description_version_id,
      created_at::TIMESTAMP                 AS created_at,
      updated_at::TIMESTAMP                 AS updated_at

    FROM source

)

SELECT  *
FROM renamed
