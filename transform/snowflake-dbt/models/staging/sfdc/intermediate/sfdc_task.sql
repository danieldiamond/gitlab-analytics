WITH source AS (

    SELECT *
    FROM {{ ref('sfdc_task_source') }}

)

SELECT *
FROM source
WHERE is_deleted = FALSE
