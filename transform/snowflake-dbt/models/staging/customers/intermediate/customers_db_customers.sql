WITH source AS (

    SELECT {{ hash_sensitive_columns('customers_db_customers_source') }}
    FROM {{ ref('customers_db_customers_source') }}

)

SELECT *
FROM source
