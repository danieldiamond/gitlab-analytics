{{ config({
    "schema": "analytics",
    "post-hook": "grant select on {{this}} to role reporter"
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

        --metadata
      createddate       AS campaign_member_created_date,
      systemmodstamp

    FROM source
    WHERE isdeleted = FALSE

)

SELECT *
FROM renamed

