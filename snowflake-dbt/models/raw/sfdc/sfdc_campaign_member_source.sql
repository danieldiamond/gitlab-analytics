WITH source AS (

    SELECT *
    FROM {{ source('salesforce', 'campaign_member') }}


), renamed AS(

    SELECT
      id::varchar                      AS campaign_member_id,

        --keys
      campaignid::varchar              AS campaign_id,
      leadorcontactid::varchar         AS lead_or_contact_id,

        --info
      type                             AS campaign_member_type,
      hasresponded::boolean            AS campaign_member_has_responded,
      firstrespondeddate::date         AS campaign_member_response_date,
      mql_after_campaign__c::boolean   AS is_mql_after_campaign,

        --metadata
      createddate::date                AS campaign_member_created_date,
      systemmodstamp,

      isdeleted                        AS is_deleted

    FROM source
)

SELECT *
FROM renamed
