WITH source AS (

	SELECT *
	FROM {{ var("database") }}.gitlab_dotcom.project_auto_devops

), renamed AS (

    SELECT

      project_id :: integer              as project_id,
      created_at :: timestamp            as project_auto_devops_created_at,
      updated_at :: timestamp            as project_auto_devops_updated_at,
      enabled :: boolean                 as has_auto_devops_enabled

    FROM source

)

SELECT *
FROM renamed