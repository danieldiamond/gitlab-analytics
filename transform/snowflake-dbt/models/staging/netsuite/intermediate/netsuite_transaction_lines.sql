{{ config({
    "materialized": "ephemeral"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ ref('netsuite_transaction_lines_source') }}

)

SELECT *
FROM source
WHERE non_posting_line != 'yes' --removes transactions not intended for posting
