{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) as rank_in_key
  FROM {{ source('gitlab_dotcom', 'ci_stages') }}
  WHERE created_at IS NOT NULL

), renamed AS (
  
  SELECT

    id::INTEGER           AS ci_stage_id,
    project_id::INTEGER   AS project_id,
    pipeline_id::INTEGER  AS pipeline_id,
    created_at::TIMESTAMP AS created_at,
    updated_at::TIMESTAMP AS updated_at,
    name::VARCHAR         AS ci_stage_name,
    status::INTEGER       AS ci_stage_status,
    lock_version::INTEGER AS lock_version,
    position::INTEGER     AS position

  FROM source
  WHERE rank_in_key = 1

)

SELECT *
FROM renamed
