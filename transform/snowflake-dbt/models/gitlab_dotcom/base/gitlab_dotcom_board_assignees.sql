{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

	SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) as rank_in_key
  FROM {{ source('gitlab_dotcom', 'board_assignees') }}

), renamed AS (

    SELECT
      id :: integer                 as board_assignee_relation_id,
      board_id :: integer           as board_id,
      assignee_id :: integer        as board_assignee_id

    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed