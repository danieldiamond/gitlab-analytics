WITH source AS (

    SELECT *
    FROM {{ source('salesforce', 'event') }}

), renamed AS (

    SELECT
      id                                    AS event_id,
        
        --keys
      accountid::VARCHAR                    AS account_id,
      ownerid::VARCHAR                      AS owner_id,
      whoid::VARCHAR                        AS lead_or_contact_id,
        
        --info      
      subject::VARCHAR                      AS event_subject,
      activitydate::DATE                    AS event_date,
      activity_source__c::VARCHAR           AS event_source,
      outreach_meeting_type__c::VARCHAR     AS outreach_meeting_type,
      type::VARCHAR                         AS event_type,
      eventsubtype::VARCHAR                 AS event_sub_type,
      event_disposition__c::VARCHAR         AS event_disposition,
      createddate::TIMESTAMP                 AS created_at,

      isdeleted::BOOLEAN                    AS is_deleted

    FROM source
)

SELECT *
FROM renamed
