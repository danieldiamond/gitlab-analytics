WITH source AS (

	SELECT *
  FROM {{ref('zuora_country_geographic_region')}}

)

SELECT *
FROM source
