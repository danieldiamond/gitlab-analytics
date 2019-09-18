{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'ci_build_trace_section_names') }}

), renamed AS (

    SELECT
    id::INTEGER         AS ci_build_id, 
    project_id::INTEGER AS ci_build_project_id,
    name                AS ci_build_name

    FROM source

)


SELECT *
FROM renamed