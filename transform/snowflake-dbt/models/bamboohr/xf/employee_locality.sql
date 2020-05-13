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

)

SELECT 
  locality.employee_number,
  locality.employee_id,
  locality.updated_at,
  locality.locality                                              AS bamboo_locality,
  COALESCE(location_factor_yaml.location_factor, 
           location_factor_yaml_everywhere_else.location_factor) AS location_factor
FROM locality
LEFT JOIN location_factor_yaml
  ON lower(locality.locality) = LOWER(CONCAT(location_factor_yaml.area,', ', location_factor_yaml.country))
  AND locality.updated_at = location_factor_yaml.snapshot_date
LEFT JOIN location_factor_yaml_everywhere_else
  ON lower(locality.locality) = LOWER(CONCAT(location_factor_yaml_everywhere_else.area,', ', location_factor_yaml_everywhere_else.country))
  AND locality.updated_at = location_factor_yaml_everywhere_else.snapshot_date
