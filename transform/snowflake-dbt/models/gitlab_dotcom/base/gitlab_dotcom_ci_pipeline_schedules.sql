{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rank_in_key
  FROM {{ source('gitlab_dotcom', 'ci_pipeline_schedules') }}

), renamed AS (

    SELECT
      id::INTEGER            AS ci_pipeline_schedule_id, 
      description            AS description, 
      ref                    AS ref, 
      cron                   AS cron, 
      cron_timezone          AS cron_timezone, 
      next_run_at::TIMESTAMP AS ci_pipeline_schedule_next_run_at, 
      project_id::INTEGER    AS ci_pipeline_schedule_project_id, 
      owner_id::INTEGER      AS ci_pipeline_schedule_owner_id, 
      active                 AS active, 
      created_at::TIMESTAMP  AS ci_pipeline_schedule_created_at, 
      updated_at::TIMESTAMP  AS ci_pipeline_schedule_updated_at


    FROM source
    WHERE rank_in_key = 1

)


SELECT *
FROM renamed
