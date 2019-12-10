{{config({
    "schema": "staging"
  })
}}

WITH source AS (

    SELECT *
    FROM {{ source('salesforce', 'lead') }}



), renamed AS(

    SELECT
        --id
        id                          AS lead_id,
        name                        AS lead_name,
        email                       AS lead_email,

        --keys
        masterrecordid              AS master_record_id,
        convertedaccountid          AS converted_account_id,
        convertedcontactid          AS converted_contact_id,
        convertedopportunityid      AS converted_opportunity_id,
        ownerid                     AS owner_id,
        recordtypeid                AS record_type_id,
        round_robin_id__c           AS round_robin_id,
        instance_uuid__c            AS instance_uuid,


        --lead info
        isconverted                 AS is_converted,
        converteddate               AS converted_date,
        title                       AS title,
        donotcall                   AS is_do_not_call,
        hasoptedoutofemail          AS has_opted_out_email,
        emailbounceddate            AS email_bounced_date,
        emailbouncedreason          AS email_bounced_reason,

        leadsource                  AS lead_source,
        lead_from__c                AS lead_from,
        lead_source_type__c         AS lead_source_type,
        lead_conversion_approval_status__c
                                    AS lead_conversiona_approval_status,

        street                      AS street,
        city                        AS city,
        state                       AS state,
        statecode                   AS state_code,
        country                     AS country,
        countrycode                 AS country_code,
        postalcode                  AS postal_code,
        latitude                    AS latitude,
        longitude                   AS longitude,

        -- info
        requested_contact__c        AS requested_contact,
        company                     AS company,
        buying_process_for_procuring_gitlab__c
                                    AS buying_process,
        industry                    AS industry,
        region__c                   AS region,
        largeaccount__c             AS is_large_account,
        outreach_stage__c           AS outreach_stage,
        interested_in_gitlab_ee__c  AS is_interested_gitlab_ee,
        interested_in_hosted_solution__c
                                    AS is_interested_in_hosted,
        lead_assigned_datetime__c::datetime   
                                    AS assigned_datetime,
        matched_account_top_list__c AS matched_account_top_list,
        mql_date__c                 AS marketo_qualified_lead_date,
        mql_datetime__c             AS marketo_qualified_lead_datetime,
        inquiry_datetime__c         AS inquiry_datetime,
        accepted_datetime__c        AS accepted_datetime,
        qualifying_datetime__c      AS qualifying_datetime,
        qualified_datetime__c       AS qualified_datetime,
        unqualified_datetime__c     AS unqualified_datetime,
        nurture_datetime__c         AS nurture_datetime,
        bad_data_datetime__c        AS bad_data_datetime,
        web_portal_purchase_datetime__c AS web_portal_purchase_datetime,
        {{ sales_segment_cleaning('sales_segmentation__c') }} AS sales_segmentation,
        mkto71_Lead_Score__c        AS person_score,
        status                      AS lead_status,

        {{  sfdc_source_buckets('leadsource') }}


        --path factory info
        pathfactory_experience_name__c   
                                    AS pathfactory_experience_name,
        pathfactory_engagement_score__c   
                                    AS pathfactory_engagement_score,
        pathfactory_content_count__c   
                                    AS pathfactory_content_count,
        pathfactory_content_list__c   
                                    AS pathfactory_content_list,
        pathfactory_content_journey__c   
                                    AS pathfactory_content_journey,
        pathfactory_topic_list__c   AS pathfactory_topic_list,

        --gitlab internal

        bdr_lu__c                   AS business_development_look_up,
        business_development_rep_contact__c
                                    AS business_development_representative_contact,
        business_development_representative__c
                                    AS business_development_representative,
        competition__c              AS competition,


        --metadata
        createdbyid                 AS created_by_id,
        createddate                 AS created_date,
        lastactivitydate            AS last_activity_date,
        lastmodifiedbyid            AS last_modified_id,
        lastmodifieddate            AS last_modified_date,
        systemmodstamp


    FROM source
    WHERE isdeleted = FALSE

)

SELECT *
FROM renamed
