WITH source AS (
    SELECT *
    FROM {{ ref('sfdc_statement_of_work_source') }}
    WHERE is_deleted = FALSE
)

SELECT *
FROM source


