-- disabled model until the data starts flowing in (the source table is missing from tap_postgres)
{{
  config(
    enabled = false
  )
}}

WITH source AS (

	SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) as rank_in_key
  FROM {{ source('gitlab_dotcom', 'project_statistics') }}

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
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed