WITH location_application_answer AS (
  
  SELECT *
  FROM {{ ref('greenhouse_locations_cleaned_intermediate') }}
  
), latest_location_factor AS (
  
  SELECT *
  FROM {{ ref('location_factors_yaml_latest_area_partition') }}
  
), application_answer_loc_factor AS (

  SELECT
    location_application_answer.application_answer,
    location_application_answer.city,
    location_application_answer.state,
    location_application_answer.country,
    latest_location_factor.location_factor
  FROM location_application_answer
  LEFT JOIN latest_location_factor
    ON location_application_answer.city = latest_location_factor.city
    AND location_application_answer.state = latest_location_factor.state 
    AND location_application_answer.country = latest_location_factor.country
  ORDER BY 4 ASC
  
), null_location_factor AS (

  SELECT 
    application_answer,
    CASE
      WHEN country = 'mexico' THEN 'all'
      WHEN (city = 'all' AND state != 'all') THEN 'everywhere else'
      ELSE 'all' END                                                                                                                                        AS city,
    CASE
      WHEN country = 'mexico' THEN 'all'
      WHEN state = 'all' THEN 'everywhere else'
      ELSE state END                                                                                                                                        AS state,
    country
  FROM application_answer_loc_factor
  WHERE location_factor IS NULL

), not_null_location_factor AS (
  
  SELECT
    application_answer,
    city,
    state,
    country
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
    latest_location_factor.location_factor
  FROM union_application_answers
  LEFT JOIN latest_location_factor
    ON union_application_answers.city = latest_location_factor.city
    AND union_application_answers.state = latest_location_factor.state 
    AND union_application_answers.country = latest_location_factor.country

)

SELECT *
FROM joined
