{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY id ORDER BY UPDATED_AT DESC) as rank_in_key
  FROM {{ source('gitlab_dotcom', 'ci_pipelines') }}
  WHERE created_at IS NOT NULL

), renamed AS (
  
  SELECT
   
    id                            AS ci_pipeline_id, 
    created_at::TIMESTAMP         AS ci_pipeline_created_at, 
    updated_at::TIMESTAMP         AS ci_pipeline_updated_at, 
    tag                           AS tag, 
    yaml_errors                   AS yaml_errors, 
    committed_at::TIMESTAMP       AS ci_pipeline_committed_at, 
    project_id::INTEGER           AS ci_pipeline_project_id, 
    status                        AS status, 
    started_at::TIMESTAMP         AS ci_pipeline_started_at, 
    finished_at::TIMESTAMP        AS ci_pipeline_finished_at, 
    duration                      AS duration, 
    user_id::INTEGER              AS ci_pipeline_user_id, 
    lock_version                  AS lock_version, 
    auto_canceled_by_id::INTEGER  AS ci_pipeline_auto_canceled_by_id, 
    pipeline_schedule_id::INTEGER AS ci_pipeline_pipeline_schedule_id, 
    source                        AS source, 
    config_source                 AS config_source, 
    protected                     AS protected, 
    failure_reason                AS failure_reason, 
    iid::INTEGER                  AS ci_pipeline_iid, 
    merge_request_id::INTEGER     AS ci_pipeline_merge_request_id 
    
  FROM source
  WHERE rank_in_key = 1 

)

SELECT *
FROM renamed
