WITH source AS (

    SELECT *
    FROM {{ ref('sfdc_quote_source') }}
)

SELECT *
FROM renamed
WHERE is_deleted = FALSE