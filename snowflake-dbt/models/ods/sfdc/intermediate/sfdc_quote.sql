WITH source AS (

    SELECT *
    FROM {{ ref('sfdc_quote_source') }}
    WHERE is_deleted = FALSE
)

SELECT *
FROM source
