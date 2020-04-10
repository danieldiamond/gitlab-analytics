WITH source AS (

    SELECT *
    FROM {{ ref('sfdc_task_source') }}
    WHERE is_deleted = FALSE

)

SELECT *
FROM source

