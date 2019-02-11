WITH source AS (

	SELECT *
	FROM {{ var("database") }}.gitlab_dotcom.boards
  
), renamed AS (

    SELECT
      id :: integer              as board_id,
      project_id :: integer      as project_id,
      created_at :: timestamp    as board_created_at,
      updated_at :: timestamp    as board_updated_at,
      milestone_id :: integer    as milestone_id,
      group_id :: integer        as group_id,
      weight :: integer          as weight

    FROM source


)

SELECT *
FROM renamed