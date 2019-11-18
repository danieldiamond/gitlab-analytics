{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'boards') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

  SELECT
    id::INTEGER              AS board_id,
    project_id::INTEGER      AS project_id,
    created_at::TIMESTAMP    AS created_at,
    updated_at::TIMESTAMP    AS updated_at,
    milestone_id::INTEGER    AS milestone_id,
    group_id::INTEGER        AS group_id,
    weight::INTEGER          AS weight

  FROM source
  WHERE rank_in_key = 1

)


SELECT *
FROM renamed
