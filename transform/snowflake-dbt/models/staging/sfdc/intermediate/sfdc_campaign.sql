WITH source AS (

    SELECT *
    FROM {{ ref('sfdc_campaign_source') }}

)

SELECT *
FROM renamed
WHERE is_deleted = FALSE
