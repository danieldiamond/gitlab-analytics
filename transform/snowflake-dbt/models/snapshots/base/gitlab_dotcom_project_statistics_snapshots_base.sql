{{ config({
    "schema": "staging",
    "alias": "gitlab_dotcom_project_statistics_snapshots"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ source('snapshots', 'gitlab_dotcom_project_statistics_snapshots') }}

), renamed AS (

  SELECT 
    dbt_scd_id::VARCHAR                             AS project_statistics_snapshot_id,
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
    "DBT_VALID_FROM"::TIMESTAMP                     AS valid_from,
    "DBT_VALID_TO"::TIMESTAMP                       AS valid_to
  
  FROM source

)

SELECT *
FROM renamed
