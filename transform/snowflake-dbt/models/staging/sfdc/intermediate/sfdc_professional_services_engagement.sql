WITH source AS (
    SELECT *
    FROM {{ ref('sfdc_statement_of_work_source') }}
)

SELECT *
FROM source
WHERE is_deleted = FALSE

