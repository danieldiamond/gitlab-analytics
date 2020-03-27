WITH source AS (

    SELECT *
    FROM {{ ref('sfdc_lead_source') }}

)

SELECT *
FROM source
WHERE is_deleted = FALSE
