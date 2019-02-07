WITH source AS (

	SELECT *
	FROM raw.gitlab_dotcom.namespace_statistics

), renamed AS (

    SELECT

      id :: integer                                      as namespace_statistics_id,
      namespace_id :: integer                            as namespace_id,
      shared_runners_seconds :: integer                  as shared_runners_seconds,
      shared_runners_seconds_last_reset :: timestamp     as shared_runners_seconds_last_reset

    FROM source


)

SELECT *
FROM renamed
