{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rank_in_key
  FROM {{ source('gitlab_dotcom', 'ci_pipeline_schedule_variables') }}

), renamed AS (

    SELECT
      id::INTEGER                   AS ci_pipeline_schedule_id,
      key                           AS key,
      value                         AS value,
      encrypted_value               AS encrypted_value,
      encrypted_value_salt          AS encrypted_value_salt,
      encrypted_value_iv            AS encrypted_value_iv,
      pipeline_schedule_id::INTEGER AS ci_pipeline_schedule__id,
      created_at::TIMESTAMP         AS ci_pipeline_schedule_created_at,
      updated_at::TIMESTAMP         AS ci_pipeline_schedule_updated_at,
      variable_type                 AS variable_type

    FROM source
    WHERE rank_in_key = 1

)


SELECT *
FROM renamed
