WITH location_application_answer AS (
  
    SELECT *
    FROM {{ ref('greenhouse_locations_cleaned_intermediate') }}
    WHERE application_question_answer_created_at >= '2019-07-22'

), historical_location_factor AS (
  
    SELECT *
    FROM {{ ref('location_factors_historical_greenhouse') }}

), historical_location_factor_modified AS (

    SELECT 
      original_country_area, 
      location_factor, 
      snapshot_date
    FROM {{ ref('location_factors_historical_greenhouse') }}
    GROUP BY 1,2,3  

), application_answer_loc_factor AS (

    SELECT
      location_application_answer.*,
      COALESCE(historical_location_factor_modified.location_factor, 
               historical_location_factor.location_factor)            AS location_factor
    FROM location_application_answer
    LEFT JOIN historical_location_factor_modified
      ON location_application_answer.application_answer = historical_location_factor_modified.original_country_area
      AND DATE_TRUNC(DAY, location_application_answer.application_question_answer_created_at) = historical_location_factor_modified.snapshot_date
    LEFT JOIN historical_location_factor
      ON location_application_answer.city = historical_location_factor.city
      AND location_application_answer.state = historical_location_factor.state 
      AND location_application_answer.country = historical_location_factor.country
      AND DATE_TRUNC(DAY, location_application_answer.application_question_answer_created_at) = historical_location_factor.snapshot_date
      AND historical_location_factor_modified.location_factor IS NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY location_application_answer.application_id, job_post_id, application_answer ORDER BY location_application_answer.state) = 1

), null_location_factor AS (

    SELECT
      job_post_id,
      application_id, 
      application_answer,
      CASE
        WHEN country = 'mexico' 
          THEN 'all'
        WHEN (city = 'all' AND state != 'all') 
          THEN 'everywhere else'
        ELSE 'all' END                           AS city,
      CASE
        WHEN country = 'mexico' 
          THEN 'all'
        WHEN state = 'all' 
          THEN 'everywhere else'
        ELSE state END                           AS state,
      country,
      application_question_answer_created_at
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
      application_question_answer_created_at
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
      AND DATE_TRUNC(DAY, union_application_answers.application_question_answer_created_at) = historical_location_factor.snapshot_date
    
)

SELECT *
FROM joined
