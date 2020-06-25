WITH source AS (

    SELECT *
    FROM {{ ref('netsuite_entity_source') }}

)

SELECT *
FROM source
WHERE non_posting_line != 'yes' --removes transactions not intended for posting
