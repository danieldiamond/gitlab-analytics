{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

	SELECT *,
				ROW_NUMBER() OVER (PARTITION BY id ORDER BY UPDATED_AT DESC) as rank_in_key
  FROM {{ source('gitlab_dotcom', 'ci_stages') }}

), renamed AS (
  
  SELECT

    id::INTEGER           AS ci_stage_id,
    project_id::INTEGER   AS ci_stage_project_id,
    pipeline_id::INTEGER  AS ci_stage_pipeline_id,
    created_at::TIMESTAMP AS ci_stage_created_at,
    updated_at::TIMESTAMP AS ci_stage_updated_at,
    name                  AS name,
    status                AS status,
    lock_version          AS lock_version,
    position              AS position

  FROM source
  WHERE rank_in_key = 1

)

SELECT *
FROM renamed
