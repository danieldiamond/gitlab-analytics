WITH source AS (

	SELECT *
	FROM raw.gitlab_dotcom.epic_metrics

), renamed AS (

    SELECT
      epic_id :: integer                    as epic_id,
      created_at :: timestamp               as epic_metrics_created_at,
      updated_at :: timestamp               as epic_metrics_updated_at,
      to_timestamp(_updated_at :: int)      as epic_metrics_last_updated

    FROM source


)

SELECT *
FROM renamed