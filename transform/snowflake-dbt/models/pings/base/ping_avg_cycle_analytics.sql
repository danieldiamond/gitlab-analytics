{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ source('pings_tap_postgres', 'avg_cycle_analytics') }}

),

renamed AS (

  SELECT
    id::INTEGER                   AS avg_cycle_analytics_id,
    usage_data_id::INTEGER        AS usage_data_id,
    total::INTEGER                
    issue_average::INTEGER      
    issue_sd::INTEGER            
    issue_missing::INTEGER        
    plan_average::INTEGER 
    plan_sd::INTEGER 
    plan_missing::INTEGER 
    code_average::INTEGER 
    code_sd::INTEGER 
    code_missing::INTEGER 
    test_average::INTEGER 
    test_sd::INTEGER 
    test_missing::INTEGER 
    review_average::INTEGER 
    review_sd::INTEGER 
    review_missing::INTEGER 
    staging_average::INTEGER 
    staging_sd::INTEGER 
    staging_missing::INTEGER 
    production_average::INTEGER 
    production_sd::INTEGER 
    production_missing::INTEGER 
  FROM source
  WHERE True
)

SELECT *
FROM renamed
