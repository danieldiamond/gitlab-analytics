WITH raw_application_answer AS (
  
  SELECT 
    application_id,
    application_answer,
    DATE_TRUNC('day', application_question_answer_created_at) AS application_question_answer_created_at
  FROM {{ ref('greenhouse_application_question_answers') }}
  WHERE application_question IN ('Location',
                                 'What location are you in?')
  
), application_answer_base AS (
  
  SELECT
    application_id,
    application_answer,
    SPLIT_PART(application_answer, '-', 2)                              AS area,
    SPLIT_PART(application_answer, '-', 1)                              AS country,
    application_question_answer_created_at
  FROM raw_application_answer
  
), area_partition AS (
  
  SELECT
    application_id,
    application_answer,
    REGEXP_COUNT(area, ', ', 1)                                         AS comma_count,
    REGEXP_COUNT(area, '/', 1)                                          AS slash_count,
    area,
    country,
    application_question_answer_created_at
  FROM application_answer_base

), countries_all AS (
  
  SELECT
    application_id,
    application_answer,
    'all'                                                               AS city,
    'all'                                                               AS state,
    LOWER(country)                                                      AS country,
    application_question_answer_created_at
  FROM area_partition
  WHERE area = 'All'

), state_only AS (
  
  SELECT
    application_id,
    application_answer,
    'all'                                                               AS city,
    LOWER(area)                                                         AS state,
    LOWER(country)                                                      AS country,
    application_question_answer_created_at
  FROM area_partition
  WHERE area != 'All'
    AND comma_count = 0
    AND slash_count = 0

), multiple_states_1 AS (
  
  SELECT
    application_id,
    application_answer,
    'all'                                                               AS city,
    TRIM(LOWER(SPLIT_PART(area, '/', 1)))                               AS state,
    LOWER(country)                                                      AS country,
    application_question_answer_created_at
  FROM area_partition
  WHERE area != 'All'
    AND comma_count = 0
    AND slash_count > 0

), multiple_states_2 AS (
  
  SELECT
    application_id,
    application_answer,
    'all'                                                               AS city,
    TRIM(LOWER(SPLIT_PART(area, '/', 2)))                               AS state,
    LOWER(country)                                                      AS country,
    application_question_answer_created_at
  FROM area_partition
  WHERE area != 'All'
    AND comma_count = 0
    AND slash_count > 0

), multiple_states AS (
  
  SELECT *
  FROM multiple_states_1
  
  UNION ALL
  
  SELECT * 
  FROM multiple_states_2

), city_and_state AS (
  
  SELECT
    application_id,
    application_answer,
    TRIM(LOWER(SPLIT_PART(area, ', ', 1)))                              AS city,
    TRIM(LOWER(SPLIT_PART(area, ', ', 2)))                              AS state,
    LOWER(country)                                                      AS country,
    application_question_answer_created_at
  FROM area_partition
  WHERE area != 'All'
    AND comma_count = 1
    AND slash_count = 0

), multiple_cities_1 AS (
  
  SELECT
    application_id,
    application_answer,
    TRIM(LOWER((SPLIT_PART(SPLIT_PART(area, ', ', 1), '/', 1))))        AS city,
    TRIM(LOWER(SPLIT_PART(area, ', ', 2)))                              AS state,
    LOWER(country)                                                      AS country,
    application_question_answer_created_at
  FROM area_partition
  WHERE area != 'All'
    AND comma_count = 1
    AND slash_count > 0

), multiple_cities_2 AS (
  
  SELECT
    application_id,
    application_answer,
    TRIM(LOWER((SPLIT_PART(SPLIT_PART(area, ', ', 1), '/', 2))))        AS city,
    TRIM(LOWER(SPLIT_PART(area, ', ', 2)))                              AS state,
    LOWER(country)                                                      AS country,
    application_question_answer_created_at
  FROM area_partition
  WHERE area != 'All'
    AND comma_count = 1
    AND slash_count > 0
  
), multiple_cities AS (
  
  SELECT * 
  FROM multiple_cities_1
  WHERE state != 'missouri / kansas'
  
  UNION ALL
  
  SELECT *
  FROM multiple_cities_2
  WHERE state != 'missouri / kansas'
  
), multiple_cities_and_states_1 AS (
  
  SELECT
    application_id,
    application_answer,
    TRIM(LOWER(SPLIT_PART(area, ', ', 1)))                              AS city,
    TRIM(LOWER(SPLIT_PART(SPLIT_PART(area, ', ', 2), '/', 1)))          AS state,
    LOWER(country)                                                      AS country,
    application_question_answer_created_at
  FROM area_partition
  WHERE area != 'All'
    AND comma_count = 1
    AND slash_count > 0
    

), multiple_cities_and_states_2 AS (
  
  SELECT
    application_id,
    application_answer,
    TRIM(LOWER(SPLIT_PART(area, ', ', 1)))                              AS city,
    TRIM(LOWER(SPLIT_PART(SPLIT_PART(area, ', ', 2), '/', 2)))          AS state,
    LOWER(country)                                                      AS country,
    application_question_answer_created_at
  FROM area_partition
  WHERE area != 'All'
    AND comma_count = 1
    AND slash_count > 0

), multiple_cities_and_states AS (
  
  SELECT *
  FROM multiple_cities_and_states_1
  WHERE city = 'kansas city'
  
  UNION ALL
  
  SELECT *
  FROM multiple_cities_and_states_2
  WHERE city = 'kansas city'

), union_partitions AS (
  
  SELECT *
  FROM countries_all
  
  UNION ALL
  
  SELECT *
  FROM state_only
  
  UNION ALL
  
  SELECT *
  FROM multiple_states
  
  UNION ALL
  
  SELECT *
  FROM city_and_state
  
  UNION ALL
  
  SELECT *
  FROM multiple_cities
  
  UNION ALL
  
  SELECT *
  FROM multiple_cities_and_states

)

SELECT *
FROM union_partitions
