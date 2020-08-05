WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'ci_build_trace_section_names') }}

), renamed AS (

    SELECT
      id::NUMBER           AS ci_build_id, 
      project_id::NUMBER   AS project_id,
      name::VARCHAR         AS ci_build_name

    FROM source

)


SELECT *
FROM renamed
