{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rank_in_key
  FROM {{ source('gitlab_dotcom', 'ci_group_variables') }}

), renamed AS (

  SELECT 
    id::INTEGER             AS ci_group_variable_id, 
    key                     AS key, 
    value                   AS value, 
    group_id::INTEGER       AS ci_group_variable_group_id, 
    created_at::TIMESTAMP   AS created_at, 
    updated_at::TIMESTAMP   AS updated_at, 
    masked                  AS masked, 
    variable_type           AS variable_variable_type 
  FROM source
  WHERE rank_in_key = 1

)


SELECT *
FROM renamed
