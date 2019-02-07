WITH source AS (

	SELECT *
	FROM raw.gitlab_dotcom.project_statistics

), renamed AS (

    SELECT

      id :: integer                                     as project_statistics_id,
      project_id :: integer                             as project_id,
      namespace_id :: integer                           as namespace_id,
      commit_count :: integer                           as commit_count,
      storage_size :: integer                           as storage_size,
      repository_size :: integer                        as repository_size,
      lfs_objects_size :: integer                       as lfs_objects_size,
      build_artifacts_size :: integer                   as build_artifacts_size,
      shared_runners_seconds :: integer                 as shared_runners_seconds,

      shared_runners_seconds_last_reset :: timestamp    as last_update_started_at



    FROM source


)

SELECT *
FROM renamed