WITH source AS (

    SELECT *
    FROM {{ ref('customers_db_versions_source') }}

)

SELECT *
FROM source