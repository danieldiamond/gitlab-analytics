WITH source AS (

    SELECT *
    FROM {{ ref('customers_db_eula_requests_source') }}

)

SELECT *
FROM source