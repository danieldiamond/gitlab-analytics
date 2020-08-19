WITH source AS (

    SELECT
      geoname_id AS location_id,
      country_name AS country_name,
      country_iso_code AS iso_2_country_code
    FROM {{ ref('maxmind_countries_source') }}
) 

SELECT *
FROM source