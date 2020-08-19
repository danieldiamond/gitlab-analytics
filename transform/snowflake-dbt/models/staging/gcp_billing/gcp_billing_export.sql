WITH source AS (

    SELECT *
    FROM {{ ref('gcp_billing_export_source') }}

)

SELECT *
FROM source
