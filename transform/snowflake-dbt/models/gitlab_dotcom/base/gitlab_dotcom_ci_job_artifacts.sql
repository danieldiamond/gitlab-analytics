{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rank_in_key
  FROM {{ source('gitlab_dotcom', 'ci_job_artifacts') }}

), renamed AS (

  SELECT 
    id::INTEGER           AS ci_job_artifact_id, 
    project_id::INTEGER   AS ci_job_artifact_project_id, 
    job_id::INTEGER       AS ci_job_artifact_job_id, 
    file_type             AS file_type, 
    size                  AS size, 
    created_at::TIMESTAMP AS ci_job_artifact_created_at, 
    updated_at::TIMESTAMP AS ci_job_artifact_updated_at, 
    expire_at::TIMESTAMP  AS ci_job_artifact_expire_at, 
    file                  AS file, 
    file_store            AS file_store, 
    file_format           AS file_format, 
    file_location         AS file_location 

  FROM source
  WHERE rank_in_key = 1

)


SELECT *
FROM renamed
