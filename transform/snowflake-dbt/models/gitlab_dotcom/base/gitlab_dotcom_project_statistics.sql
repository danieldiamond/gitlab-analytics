WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'project_statistics') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1

), renamed AS (

  SELECT
    id::INTEGER                                     AS project_statistics_id,
    project_id::INTEGER                             AS project_id,
    namespace_id::INTEGER                           AS namespace_id,
    commit_count::INTEGER                           AS commit_count,
    storage_size::INTEGER                           AS storage_size,
    repository_size::INTEGER                        AS repository_size,
    lfs_objects_size::INTEGER                       AS lfs_objects_size,
    build_artifacts_size::INTEGER                   AS build_artifacts_size,
    shared_runners_seconds::INTEGER                 AS shared_runners_seconds,
    shared_runners_seconds_last_reset::TIMESTAMP    AS last_update_started_at,
    packages_size::INTEGER                          AS packages_size,
    wiki_size::INTEGER                              AS wiki_size
  FROM source

)

SELECT *
FROM renamed
