WITH source AS (

    SELECT *
    FROM {{ ref('sfdc_campaign_member_source') }}


)

SELECT *
FROM source
WHERE is_deleted = FALSE
