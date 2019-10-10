{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ source('pings_tap_postgres', 'licenses') }}

),

renamed AS (

  SELECT

  FROM source

)

SELECT *
FROM renamed
