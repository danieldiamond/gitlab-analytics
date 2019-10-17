
{{ config({
    "materialized": "incremental",
    "unique_key": "ci_build_id"
    })
}}

WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rank_in_key
  FROM {{ source('gitlab_dotcom', 'ci_builds') }}

  {% if is_incremental() %}

  WHERE updated_at >= (SELECT MAX(ci_build_updated_at) FROM {{this}})

  {% endif %}

), renamed AS (

  SELECT 
    id::INTEGER                      AS ci_build_id, 
    status                           AS status, 
    finished_at::TIMESTAMP           AS ci_build_finished_at, 
    trace                            AS trace, 
    created_at::TIMESTAMP            AS ci_build_created_at, 
    updated_at::TIMESTAMP            AS ci_build_updated_at, 
    started_at::TIMESTAMP            AS ci_build_started_at, 
    runner_id::INTEGER               AS ci_build_runner_id, 
    coverage                         AS coverage, 
    commit_id::INTEGER               AS ci_build_commit_id, 
    commands                         AS commands, 
    name                             AS name, 
    options                          AS options, 
    allow_failure                    AS allow_failure, 
    stage                            AS stage, 
    trigger_request_id::INTEGER      AS ci_build_trigger_request_id, 
    stage_idx                        AS stage_idx, 
    tag                              AS tag, 
    ref                              AS ref, 
    user_id::INTEGER                 AS ci_build_user_id, 
    TYPE                             AS type, 
    target_url                       AS target_url, 
    description                      AS description, 
    artifacts_file                   AS artifacts_file, 
    project_id::INTEGER              AS ci_build_project_id, 
    artifacts_metadata               AS artifacts_metadata, 
    erased_by_id::INTEGER            AS ci_build_erased_by_id, 
    erased_at::TIMESTAMP             AS ci_build_erased_at, 
    artifacts_expire_at::TIMESTAMP   AS ci_build_artifacts_expire_at, 
    environment                      AS environment, 
    artifacts_size                   AS artifacts_size, 
    yaml_variables                   AS yaml_variables, 
    queued_at::TIMESTAMP             AS ci_build_queued_at, 
    token                            AS token, 
    lock_version                     AS lock_version, 
    coverage_regex                   AS coverage_regex, 
    auto_canceled_by_id::INTEGER     AS ci_build_auto_canceled_by_id, 
    retried                          AS retried, 
    stage_id::INTEGER                AS ci_build_stage_id, 
    artifacts_file_store             AS artifacts_file_store, 
    artifacts_metadata_store         AS artifacts_metadata_store, 
    protected                        AS protected, 
    failure_reason                   AS failure_reason, 
    scheduled_at::TIMESTAMP          AS ci_build_scheduled_at, 
    token_encrypted                  AS token_encrypted, 
    upstream_pipeline_id::INTEGER    AS upstream_pipeline_id 
  FROM source
  WHERE rank_in_key = 1

)


SELECT *
FROM renamed
ORDER BY ci_build_updated_at
