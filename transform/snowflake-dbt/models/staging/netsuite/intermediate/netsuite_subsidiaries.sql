WITH source AS (

    SELECT *
    FROM {{ ref('netsuite_subsidiaries_source') }}

)

SELECT *
FROM source