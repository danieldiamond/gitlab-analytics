{{config({
    "schema": "staging"
  })
}}

WITH source AS (

    SELECT *
    FROM {{ ref('zendesk_sla_policies_source') }}

)

SELECT *
FROM source
