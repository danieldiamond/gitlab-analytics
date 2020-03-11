{{config({
    "schema": "staging"
  })
}}

WITH source AS (

    SELECT *
    FROM {{ ref('zendesk_tickets_source') }}
)

SELECT *
FROM source
