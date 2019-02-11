WITH source AS (

	SELECT *
	FROM {{ var("database") }}.gitlab_dotcom.project_mirror_data


), renamed AS (

    SELECT

      id :: integer                                     as project_mirror_data_id,
      project_id :: integer                             as project_id,
      retry_count :: integer                            as retry_count,
      last_update_started_at :: timestamp               as last_update_started_at,
      last_update_scheduled_at :: timestamp             as last_update_scheduled_at,
      next_execution_timestamp :: timestamp             as next_execution_timestamp



    FROM source


)

SELECT *
FROM renamed