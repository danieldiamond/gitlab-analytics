{{ config({
    "materialized":"table",
    "schema": "analytics"
    })
}}

WITH location_factors AS (
  
  SELECT *
  FROM {{ ref('greenhouse_location_factors') }}

)

SELECT
  application_answer,
  city,
  state,
  country, 
  application_question_answer_created_at,
  location_factor
FROM location_factors
