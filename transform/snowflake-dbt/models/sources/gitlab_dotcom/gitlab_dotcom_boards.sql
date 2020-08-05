WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'boards') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

  SELECT
    id::NUMBER              AS board_id,
    project_id::NUMBER      AS project_id,
    created_at::TIMESTAMP    AS created_at,
    updated_at::TIMESTAMP    AS updated_at,
    milestone_id::NUMBER    AS milestone_id,
    group_id::NUMBER        AS group_id,
    weight::NUMBER          AS weight

  FROM source

)


SELECT *
FROM renamed
