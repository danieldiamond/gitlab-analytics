WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'keys') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

    SELECT

      id::INTEGER             AS key_id,
      user_id::INTEGER        AS user_id,
      created_at::TIMESTAMP   AS created_at,
      updated_at::TIMESTAMP   AS updated_at,
      type::VARCHAR           AS type,
      public::BOOLEAN         AS is_public,
      last_used_at::TIMESTAMP AS last_updated_at

    FROM source

)

SELECT *
FROM renamed
