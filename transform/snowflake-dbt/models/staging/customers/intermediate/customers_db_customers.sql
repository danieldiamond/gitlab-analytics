WITH source AS (

    SELECT *
    FROM {{ ref('customers_db_customers_source') }}

)

SELECT *
FROM source
