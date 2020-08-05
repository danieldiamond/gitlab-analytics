WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'board_assignees') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1

), renamed AS (

  SELECT
    id::NUMBER                 AS board_assignee_relation_id,
    board_id::NUMBER           AS board_id,
    assignee_id::NUMBER        AS board_assignee_id

  FROM source

)

SELECT *
FROM renamed
