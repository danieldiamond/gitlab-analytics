WITH source AS (

    SELECT *
    FROM {{ ref('customers_db_orders_source') }}

)

SELECT *
FROM source