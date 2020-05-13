WITH locality AS (

  SELECT *
  FROM {{ ref('bamboohr_locality') }}

), location_factor AS (

    SELECT *
    FROM {{ ref('location_factor_yaml_historical') }}

), location_factor_everywhere_else AS (

    SELECT *
    FROM {{ ref('location_factor_yaml_historical') }}
    WHERE LOWER(area) LIKE '%everywhere else%'

)

SELECT 
  locality.employee_number,
  locality.employee_id,
  locality.uploaded_at,
  locality.locality,
  location_factor_yaml.*,
  LOWER(CONCAT(location_factor_yaml_everywhere_else.area,', ', location_factor_yaml_everywhere_else.country)),
  location_factor_yaml_everywhere_else.location_factor
FROM locality
LEFT JOIN location_factor_yaml
  ON lower(locality.locality) = LOWER(CONCAT(location_factor_yaml.area,', ', location_factor_yaml.country))
  AND locality.uploaded_at = location_factor_yaml.snapshot_date
LEFT JOIN location_factor_yaml_everywhere_else
  ON lower(locality.locality) = LOWER(CONCAT(location_factor_yaml_everywhere_else.area,', ', location_factor_yaml_everywhere_else.country))
  AND locality.uploaded_at = location_factor_yaml_everywhere_else.snapshot_date
{# where location_factor_yaml.location_factor is null  
and location_factor_yaml_everywhere_else.location_factor is null #}