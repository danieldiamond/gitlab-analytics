WITH source AS (

	SELECT *
	FROM {{ var("database") }}.gitlab_dotcom.label_priorities

), renamed AS (

    SELECT

      id :: integer                           as label_priority_id,
      project_id :: integer                   as project_id,
      label_id :: integer                     as label_id,
      priority :: integer                     as priority,
      created_at :: timestamp                 as label_priority_created_at,
      updated_at :: timestamp                 as label_priority_updated_at


    FROM source


)

SELECT *
FROM renamed