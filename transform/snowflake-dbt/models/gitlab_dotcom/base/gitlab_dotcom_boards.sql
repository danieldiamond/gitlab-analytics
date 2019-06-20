WITH source AS (

	SELECT *,
				ROW_NUMBER() OVER (PARTITION BY id ORDER BY UPDATED_AT DESC) as rank_in_key
  FROM {{ source('gitlab_dotcom', 'boards') }}

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
    WHERE rank_in_key = 1

)


SELECT *
FROM renamed
