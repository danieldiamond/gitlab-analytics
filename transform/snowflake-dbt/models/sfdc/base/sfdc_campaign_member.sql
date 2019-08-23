{{ config({
    "materialized": "table"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('salesforce', 'campaign_member') }}


), renamed AS(

    SELECT
      id                AS campaign_member_id,

        --keys
      campaignid        AS campaign_id,
      leadorcontactid   AS lead_or_contact_id,

        --info
      type              AS campaign_member_type,
      MQL_After_Campaign__c
                        AS mql_after_campaign,

        --metadata
      createddate       AS campaign_member_created_date,
      systemmodstamp

    FROM source
    WHERE isdeleted = FALSE

)

SELECT *
FROM renamed
