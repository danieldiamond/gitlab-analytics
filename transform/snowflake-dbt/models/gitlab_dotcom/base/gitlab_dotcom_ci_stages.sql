{{ config({
    "materialized": "incremental",
    "unique_key": "ci_stage_id",
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'ci_stages') }}
  WHERE created_at IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
  
    {% if is_incremental() %}

    AND updated_at >= (SELECT MAX(updated_at) FROM {{this}})

    {% endif %}

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

)

SELECT *
FROM renamed
ORDER BY updated_at
