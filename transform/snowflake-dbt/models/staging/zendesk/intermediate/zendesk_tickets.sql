{{config({
    "schema": "staging"
  })
}}

WITH source AS (

    SELECT *
    FROM {{ source('zendesk_tickets_source') }}
)

SELECT *
FROM source
