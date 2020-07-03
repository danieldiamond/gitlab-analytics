{{ config({
    "materialized": "ephemeral"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ ref('netsuite_vendors_source') }}

)

SELECT *
FROM source