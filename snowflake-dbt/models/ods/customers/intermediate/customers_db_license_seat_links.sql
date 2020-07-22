WITH source AS (

    SELECT *
    FROM {{ ref('customers_db_license_seat_links_source') }}

)

SELECT *
FROM source