-- disabled model until the data starts flowing in (the source table is missing from tap_postgres)
{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT
   *,
   ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) AS rank_in_key
  FROM {{ source('gitlab_dotcom', 'project_statistics') }}

), renamed AS (

    SELECT

      id :: integer                                     AS project_statistics_id,
      project_id :: integer                             AS project_id,
      namespace_id :: integer                           AS namespace_id,
      commit_count :: integer                           AS commit_count,
      storage_size :: integer                           AS storage_size,
      repository_size :: integer                        AS repository_size,
      lfs_objects_size :: integer                       AS lfs_objects_size,
      build_artifacts_size :: integer                   AS build_artifacts_size,
      shared_runners_seconds :: integer                 AS shared_runners_seconds,

      shared_runners_seconds_last_reset :: timestamp    AS last_update_started_at



    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed