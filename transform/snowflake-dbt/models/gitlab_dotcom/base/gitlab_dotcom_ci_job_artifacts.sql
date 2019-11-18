{{ config({
    "materialized": "incremental",
    "unique_key": "ci_job_artifact_id",
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'ci_job_artifacts') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

  {% if is_incremental() %}

  WHERE updated_at >= (SELECT MAX(ci_job_artifact_updated_at) FROM {{this}})

  {% endif %}

), renamed AS (

  SELECT 
    id::INTEGER           AS ci_job_artifact_id, 
    project_id::INTEGER   AS project_id, 
    job_id::INTEGER       AS ci_job_id, 
    file_type             AS file_type, 
    size                  AS size, 
    created_at::TIMESTAMP AS created_at, 
    updated_at::TIMESTAMP AS updated_at, 
    expire_at::TIMESTAMP  AS expire_at, 
    file                  AS file, 
    file_store            AS file_store, 
    file_format           AS file_format, 
    file_location         AS file_location 

  FROM source
  WHERE rank_in_key = 1

)


SELECT *
FROM renamed
ORDER BY ci_job_artifact_updated_at
