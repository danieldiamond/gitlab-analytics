WITH source AS (

	SELECT *
	FROM raw.gitlab_dotcom.project_import_data

), renamed AS (

    SELECT

      id :: integer                      as project_import_relation_id,
      project_id :: integer              as project_id

    FROM source


)

SELECT *
FROM renamed