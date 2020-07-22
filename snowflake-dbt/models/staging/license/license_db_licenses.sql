{{ config({
    "materialized": "view"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ ref('license_db_licenses_source') }}

)

SELECT *
FROM source
