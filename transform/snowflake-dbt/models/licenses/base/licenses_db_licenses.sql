{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY UPDATED_AT DESC) AS rank_in_key
  FROM {{ source('licenses', 'licenses_db_licenses') }}

), renamed AS (

  SELECT DISTINCT
    id::INTEGER                      AS license_id
 FROM source
 WHERE rank_in_key = 1
)

SELECT *
FROM renamed
