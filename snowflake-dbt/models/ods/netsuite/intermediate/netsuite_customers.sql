WITH source AS (

    SELECT *
    FROM {{ ref('netsuite_customers_source') }}

)

SELECT *
FROM source
WHERE is_fivetran_deleted = FALSE