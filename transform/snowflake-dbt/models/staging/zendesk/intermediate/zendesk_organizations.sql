{{config({
    "schema": "staging"
  })
}}

WITH source AS (

    SELECT *
    FROM {{ ref('zendesk_organizations_source') }}

)

SELECT *
FROM source
