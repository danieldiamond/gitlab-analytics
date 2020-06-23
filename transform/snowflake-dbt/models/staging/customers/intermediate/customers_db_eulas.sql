WITH source AS (

    SELECT *
    FROM {{ ref('customers_db_eulas_source') }}

)

SELECT *
FROM source