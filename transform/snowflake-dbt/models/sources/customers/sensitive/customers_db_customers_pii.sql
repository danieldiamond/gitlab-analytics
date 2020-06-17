WITH source AS (

    SELECT *
    FROM {{ ref('customers_db_customers') }}

), customers_db_customers_pii AS (

    SELECT
      customer_id,
      {{ nohash_sensitive_columns('customers_db_customers', 'customer_last_name') }},
      {{ nohash_sensitive_columns('customers_db_customers', 'customer_first_name') }},
      {{ nohash_sensitive_columns('customers_db_customers', 'customer_email') }}
    FROM source

)

SELECT *
FROM customers_db_customers_pii
