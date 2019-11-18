
{{ config({
    "materialized": "incremental",
    "unique_key": "ci_build_id",
    "schema": "staging"
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
    id::INTEGER                       AS ci_build_id, 
    status::VARCHAR                   AS status, 
    finished_at::TIMESTAMP            AS finished_at, 
    trace::VARCHAR                    AS trace, 
    created_at::TIMESTAMP             AS created_at, 
    updated_at::TIMESTAMP             AS updated_at, 
    started_at::TIMESTAMP             AS started_at, 
    runner_id::INTEGER                AS ci_build_runner_id, 
    coverage::VARCHAR                 AS coverage, 
    commit_id::INTEGER                AS ci_build_commit_id, 
    commands::VARCHAR                 AS commands, 
    name::VARCHAR                     AS ci_build_name, 
    options::VARCHAR                  AS options, 
    allow_failure::VARCHAR            AS allow_failure, 
    stage::VARCHAR                    AS stage, 
    trigger_request_id::INTEGER       AS ci_build_trigger_request_id, 
    stage_idx::INTEGER                AS stage_idx, 
    tag::VARCHAR                      AS tag, 
    ref::VARCHAR                      AS ref, 
    user_id::INTEGER                  AS ci_build_user_id, 
    type::VARCHAR                     AS type, 
    target_url::VARCHAR               AS target_url, 
    description::VARCHAR              AS description, 
    artifacts_file::VARCHAR           AS artifacts_file, 
    project_id::INTEGER               AS ci_build_project_id, 
    artifacts_metadata::VARCHAR       AS artifacts_metadata, 
    erased_by_id::INTEGER             AS ci_build_erased_by_id, 
    erased_at::TIMESTAMP              AS ci_build_erased_at, 
    artifacts_expire_at::TIMESTAMP    AS ci_build_artifacts_expire_at, 
    environment::VARCHAR              AS environment, 
    artifacts_size::VARCHAR           AS artifacts_size, 
    yaml_variables::VARCHAR           AS yaml_variables, 
    queued_at::TIMESTAMP              AS ci_build_queued_at, 
    token::VARCHAR                    AS token, 
    lock_version::VARCHAR             AS lock_version, 
    coverage_regex::VARCHAR           AS coverage_regex, 
    auto_canceled_by_id::INTEGER      AS ci_build_auto_canceled_by_id, 
    retried::BOOLEAN                  AS retried, 
    stage_id::INTEGER                 AS ci_build_stage_id, 
    artifacts_file_store::VARCHAR     AS artifacts_file_store, 
    artifacts_metadata_store::VARCHAR AS artifacts_metadata_store, 
    protected::BOOLEAN                AS protected, 
    failure_reason::VARCHAR           AS failure_reason, 
    scheduled_at::TIMESTAMP           AS ci_build_scheduled_at, 
    token_encrypted::VARCHAR          AS token_encrypted, 
    upstream_pipeline_id::INTEGER     AS upstream_pipeline_id 
  FROM source
  WHERE rank_in_key = 1

)


SELECT *
FROM renamed
ORDER BY ci_build_updated_at
