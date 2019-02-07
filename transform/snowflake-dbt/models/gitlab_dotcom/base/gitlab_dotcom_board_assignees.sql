WITH source AS (

	SELECT *
	FROM raw.gitlab_dotcom.board_assignees

), renamed AS (

    SELECT
      id :: integer                 as board_assignee_relation_id,
      board_id :: integer           as board_id,
      assignee_id :: integer        as board_assignee_id

    FROM source


)

SELECT *
FROM renamed