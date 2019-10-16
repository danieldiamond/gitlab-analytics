{{ config({
    "materialized": "incremental",
    "unique_key": "ci_pipeline_id"
    })
}}

WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) as rank_in_key
  FROM {{ source('gitlab_dotcom', 'ci_pipelines') }}
  WHERE created_at IS NOT NULL
  
    {% if is_incremental() %}

    AND updated_at >= (SELECT MAX(updated_at) FROM {{this}})

    {% endif %}

), renamed AS (
  
  SELECT
    id::INTEGER                   AS ci_pipeline_id, 
    created_at::TIMESTAMP         AS created_at, 
    updated_at::TIMESTAMP         AS updated_at,
    ref::VARCHAR                  AS ref,
    tag::BOOLEAN                  AS has_tag, 
    yaml_errors::VARCHAR          AS yaml_errors, 
    committed_at::TIMESTAMP       AS committed_at, 
    project_id::INTEGER           AS project_id, 
    status::VARCHAR               AS status, 
    started_at::TIMESTAMP         AS started_at, 
    finished_at::TIMESTAMP        AS finished_at, 
    duration::INTEGER             AS ci_pipeline_duration, 
    user_id::INTEGER              AS user_id, 
    lock_version::INTEGER         AS lock_version, 
    auto_canceled_by_id::INTEGER  AS auto_canceled_by_id, 
    pipeline_schedule_id::INTEGER AS pipeline_schedule_id, 
    source::INTEGER               AS ci_pipeline_source, 
    config_source::INTEGER        AS config_source, 
    protected::BOOLEAN            AS is_protected, 
    failure_reason::VARCHAR       AS failure_reason, 
    iid::INTEGER                  AS ci_pipeline_iid, 
    merge_request_id::INTEGER     AS merge_request_id 
  FROM source
  WHERE rank_in_key = 1 

)

SELECT *
FROM renamed
