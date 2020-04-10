{# {{ config({
    "schema": "temporary"
    })
}} #}

WITH location_factor_base AS (
  
  SELECT *
  FROM {{ ref('location_factors_yaml_historical') }} 

), area_partition AS (
  
  SELECT
    REGEXP_COUNT(area, ', ', 1)                                         AS comma_count,
    REGEXP_COUNT(area, '/', 1)                                          AS slash_count,
    area                                                                AS original_area,
    country                                                             AS original_country,
    location_factor,
    snapshot_date
  FROM location_factor_base
     
), countries_all AS (
  
  SELECT
    'all'                                                               AS city,
    'all'                                                               AS state,
    LOWER(original_country)                                             AS country,
    area_partition.*
  FROM area_partition
  WHERE original_area = 'All'
  
), state_only AS (
  
  SELECT
    'all'                                                               AS city,
    LOWER(original_area)                                                AS state,
    LOWER(original_country)                                             AS country,
    area_partition.*
  FROM area_partition
  WHERE original_area != 'All'
    AND comma_count = 0
    AND slash_count = 0

), multiple_states_1 AS (
  
  SELECT
    'all'                                                               AS city,
    TRIM(LOWER(SPLIT_PART(original_area, '/', 0)))                      AS state,
    LOWER(original_country)                                             AS country,
     area_partition.*
  FROM area_partition
  WHERE original_area != 'All'
    AND comma_count = 0
    AND slash_count > 0

  
), multiple_states_2 AS (
  
  SELECT
    'all'                                                               AS city,
    TRIM(LOWER(SPLIT_PART(original_area, '/', 2)))                      AS state,
    LOWER(original_country)                                             AS country,
    area_partition.*
  FROM area_partition
  WHERE original_area != 'All'
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
    TRIM(LOWER(SPLIT_PART(original_area, ', ', 0)))                              AS city,
    TRIM(LOWER(SPLIT_PART(original_area, ', ', -1)))                             AS state,
    LOWER(original_country)                                                      AS country,
    area_partition.*
  FROM area_partition
  WHERE original_area != 'All'
    AND comma_count = 1
    AND slash_count = 0
  
), multiple_cities_1 AS (
  
  SELECT
    TRIM(LOWER((SPLIT_PART(SPLIT_PART(original_area, ', ', 0), '/', 0))))        AS city,
    TRIM(LOWER(SPLIT_PART(original_area, ', ', -1)))                             AS state,
    LOWER(original_country)                                                      AS country,
    area_partition.*
  FROM area_partition
  WHERE original_area != 'All'
    AND comma_count = 1
    AND slash_count > 0

  
), multiple_cities_2 AS (
  
  SELECT
    TRIM(LOWER((SPLIT_PART(SPLIT_PART(original_area, ', ', 0), '/', 2))))        AS city,
    TRIM(LOWER(SPLIT_PART(original_area, ', ', -1)))                             AS state,
    LOWER(original_country)                                                      AS country,
    area_partition.*
  FROM area_partition
  WHERE original_area != 'All'
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
    TRIM(LOWER(SPLIT_PART(original_area, ', ', 0)))                              AS city,
    TRIM(LOWER(SPLIT_PART(SPLIT_PART(original_area, ', ', -1), '/', 0)))         AS state,
    LOWER(original_country)                                                      AS country,
    area_partition.*
  FROM area_partition
  WHERE original_area != 'All'
    AND comma_count = 1
    AND slash_count > 0
    

), multiple_cities_and_states_2 AS (
  
  SELECT
    TRIM(LOWER(SPLIT_PART(original_area, ', ', 0)))                              AS city,
    TRIM(LOWER(SPLIT_PART(SPLIT_PART(original_area, ', ', -1), '/', -1)))        AS state,
    LOWER(original_country)                                                      AS country,
    area_partition.*
  FROM area_partition
  WHERE original_area != 'All'
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