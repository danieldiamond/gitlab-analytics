WITH location_application_answer AS (
  
  SELECT *
  FROM {{ ref('greenhouse_locations_cleaned_intermediate') }}
  

), historical_location_factor AS (
  
  SELECT *
  FROM {{ ref('location_factors_historical_greenhouse') }}
  
), application_answer_loc_factor AS (

  SELECT
    location_application_answer.*,
    historical_location_factor.location_factor
  FROM location_application_answer
  LEFT JOIN historical_location_factor
    ON location_application_answer.city = historical_location_factor.city
    AND location_application_answer.state = historical_location_factor.state 
    AND location_application_answer.country = historical_location_factor.country
    AND location_application_answer.location_factor_snapshot_date = historical_location_factor.snapshot_date
  
), null_location_factor AS (

  SELECT
    job_post_id,
    application_id, 
    application_answer,
    CASE
      WHEN country = 'mexico' THEN 'all'
      WHEN (city = 'all' AND state != 'all') THEN 'everywhere else'
      ELSE 'all' END                                                                                                                                        AS city,
    CASE
      WHEN country = 'mexico' THEN 'all'
      WHEN state = 'all' THEN 'everywhere else'
      ELSE state END                                                                                                                                        AS state,
    country,
    application_question_answer_created_at,
    location_factor_snapshot_date
  FROM application_answer_loc_factor
  WHERE location_factor IS NULL

), not_null_location_factor AS (
  
  SELECT
    job_post_id,
    application_id,
    application_answer,
    city,
    state,
    country,
    application_question_answer_created_at,
    location_factor_snapshot_date
  FROM application_answer_loc_factor
  WHERE location_factor IS NOT NULL
  
), union_application_answers AS (
  
  SELECT *
  FROM null_location_factor
  
  UNION ALL
  
  SELECT *
  FROM not_null_location_factor
  
), joined AS (
    
  SELECT
    union_application_answers.*,
    historical_location_factor.location_factor
  FROM union_application_answers
  LEFT JOIN historical_location_factor
    ON union_application_answers.city = historical_location_factor.city
    AND union_application_answers.state = historical_location_factor.state 
    AND union_application_answers.country = historical_location_factor.country
    AND union_application_answers.location_factor_snapshot_date = historical_location_factor.snapshot_date

)

SELECT *
FROM joined
