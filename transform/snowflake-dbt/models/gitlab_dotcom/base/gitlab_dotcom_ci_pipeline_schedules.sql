WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'ci_pipeline_schedules') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

    SELECT
      id::INTEGER            AS ci_pipeline_schedule_id, 
      description            AS ci_pipeline_schedule_description, 
      ref                    AS ref, 
      cron                   AS cron, 
      cron_timezone          AS cron_timezone, 
      next_run_at::TIMESTAMP AS next_run_at, 
      project_id::INTEGER    AS project_id, 
      owner_id::INTEGER      AS owner_id, 
      active                 AS active, 
      created_at::TIMESTAMP  AS created_at, 
      updated_at::TIMESTAMP  AS updated_at


    FROM source

)


SELECT *
FROM renamed
