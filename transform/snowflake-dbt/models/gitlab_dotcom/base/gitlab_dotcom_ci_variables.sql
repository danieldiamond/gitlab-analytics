{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY id ORDER BY UPDATED_AT DESC) as rank_in_key
  FROM {{ source('gitlab_dotcom', 'ci_variables') }}

), renamed AS (
  
  SELECT 
  
    id ::INTEGER         AS ci_variables_id, 
    key                  AS key, 
    value                AS value, 
    project_id ::INTEGER AS ci_variables_project_id, 
    protected            AS protected, 
    environment_scope    AS environment_scope, 
    masked               AS masked, 
    variable_type        AS variable_type 
  
  FROM source
  
)

SELECT *
FROM renamed
