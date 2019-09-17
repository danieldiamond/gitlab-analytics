{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT
    *
  FROM {{ source('gitlab_dotcom', 'ci_pipeline_variables') }}

), renamed AS (

    SELECT 
      id::INTEGEr          AS ci_pipeline_variable_id, 
      key                  AS key, 
      value                AS value, 
      encrypted_value      AS encrypted_value, 
      encrypted_value_salt AS encrypted_value_salt, 
      encrypted_value_iv   AS encrypted_value_iv, 
      pipeline_id::INTEGER AS ci_pipeline_id, 
      variable_type        AS variable_type

    FROM source

)


SELECT *
FROM renamed
