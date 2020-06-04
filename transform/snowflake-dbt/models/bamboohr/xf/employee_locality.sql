WITH locality AS (

  SELECT *
  FROM {{ ref('bamboohr_locality') }}

), location_factor_yaml AS (

    SELECT *
    FROM {{ ref('location_factors_yaml_historical') }} 

), location_factor_yaml_everywhere_else AS (

    SELECT *
    FROM {{ ref('location_factors_yaml_historical') }}
    WHERE LOWER(area) LIKE '%everywhere else%'

), location_factor_mexico AS (

    SELECT *
    FROM {{ ref('location_factors_yaml_historical') }}
    WHERE country ='Mexico'
      AND area IN ('Everywhere else','All')

)

SELECT 
  locality.employee_number,
  locality.employee_id,
  locality.updated_at,
  locality.locality                                                 AS bamboo_locality,
  COALESCE(location_factor_yaml.location_factor/100, 
           location_factor_yaml_everywhere_else.location_factor/100,
           location_factor_mexico.location_factor/100)              AS location_factor
FROM locality
LEFT JOIN location_factor_yaml
  ON LOWER(locality.locality) = LOWER(CONCAT(location_factor_yaml.area,', ', location_factor_yaml.country))
  AND locality.updated_at = location_factor_yaml.snapshot_date
LEFT JOIN location_factor_yaml_everywhere_else
  ON LOWER(locality.locality) = LOWER(CONCAT(location_factor_yaml_everywhere_else.area,', ', location_factor_yaml_everywhere_else.country))
  AND locality.updated_at = location_factor_yaml_everywhere_else.snapshot_date
LEFT JOIN location_factor_mexico
  ON IFF(locality.locality LIKE '%Mexico%', 'Mexico',NULL) = location_factor_mexico.country
  AND locality.updated_at = location_factor_mexico.snapshot_date
--The naming convention for Mexico is all in bamboohr and everywhere else in the location factor yaml file accounting for these cases by making this the last coalesce