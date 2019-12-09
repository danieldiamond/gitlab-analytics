WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rank_in_key
  FROM {{ source('gitlab_dotcom', 'boards') }}

), renamed AS (

    SELECT
      id::INTEGER              AS board_id,
      project_id::INTEGER      AS project_id,
      created_at::TIMESTAMP    AS board_created_at,
      updated_at::TIMESTAMP    AS board_updated_at,
      milestone_id::INTEGER    AS milestone_id,
      group_id::INTEGER        AS group_id,
      weight::INTEGER          AS weight

    FROM source
    WHERE rank_in_key = 1

)


SELECT *
FROM renamed
