{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

	SELECT *
  FROM {{ source('gitlab_dotcom', 'ci_pipeline_chat_data') }}

), renamed AS (

    SELECT 
      id::INTEGER         AS ci_build_metadata_id,
      build_id::INTEGER   AS ci_build_id,
      project_id::INTEGER AS ci_build_project_id,
      timeout             AS timeout,
      timeout_source      AS timeout_source,
      config_options      AS config_options,
      config_variables    AS config_variables

    FROM source

)


SELECT *
FROM renamed
