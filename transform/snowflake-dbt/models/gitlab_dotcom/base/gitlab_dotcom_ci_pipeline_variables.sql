WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'ci_pipeline_variables') }}

), renamed AS (

    SELECT 
      id::INTEGER          AS ci_pipeline_variable_id, 
      key                  AS key, 
      value                AS value, 
      pipeline_id::INTEGER AS ci_pipeline_id, 
      variable_type        AS variable_type

    FROM source

)


SELECT *
FROM renamed
