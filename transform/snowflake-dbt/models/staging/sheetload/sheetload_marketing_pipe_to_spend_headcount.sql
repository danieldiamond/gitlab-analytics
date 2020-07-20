{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_marketing_pipe_to_spend_headcount_source') }}

)

SELECT *
FROM source
