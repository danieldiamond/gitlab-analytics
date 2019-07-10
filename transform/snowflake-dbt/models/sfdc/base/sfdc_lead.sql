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

        matched_account_top_list__c AS matched_account_top_list,
        mql_date__c                 AS marketo_qualified_lead_date,
        mql_datetime__c             AS marketo_qualified_lead_datetime,
        inquiry_datetime__c         AS inquiry_datetime,
        accepted_datetime__c        AS accepted_datetime,
        qualifying_datetime__c      AS qualifying_datetime,
        qualified_datetime__c       AS qualified_datetime,
        unqualified_datetime__c     AS unqualified_datetime,
        {{ sales_segment_cleaning('sales_segmentation__c') }} AS sales_segmentation,
        mkto71_Lead_Score__c        AS person_score,
        status                      AS lead_status,
 CASE
      WHEN lead_source in ('CORE Check-Up')
        THEN 'Core'
      WHEN lead_source in ('GitLab Subscription Portal', 'Gitlab.com', 'GitLab.com', 'Trial - Gitlab.com', 'Trial - GitLab.com')
        THEN 'GitLab.com'
      WHEN lead_source in ('Education', 'OSS')
        THEN 'Marketing/Community'
      WHEN lead_source in ('CE Download', 'Demo', 'Drift', 'Email Request', 'Email Subscription', 'Gated Content - General', 'Gated Content - Report', 'Gated Content - Video'
                           , 'Gated Content - Whitepaper', 'Live Event', 'Newsletter', 'Request - Contact', 'Request - Professional Services', 'Request - Public Sector'
                           , 'Security Newsletter', 'Trial - Enterprise', 'Virtual Sponsorship', 'Web Chat', 'Web Direct', 'Web', 'Webcast')
        THEN 'Marketing/Inbound'
      WHEN lead_source in ('Advertisement', 'Conference', 'Field Event', 'Owned Event')
        THEN 'Marketing/Outbound'
      WHEN lead_source in ('Clearbit', 'Datanyze', 'Leadware', 'LinkedIn', 'Prospecting - LeadIQ', 'Prospecting - General', 'Prospecting', 'SDR Generated')
        THEN 'Prospecting'
      WHEN lead_source in ('Employee Referral', 'External Referral', 'Partner', 'Word of mouth')
        THEN 'Referral'
      WHEN lead_source in ('AE Generated')
        THEN 'Sales'
      WHEN lead_source in ('DiscoverOrg')
        THEN 'DiscoverOrg'
      ELSE 'Other'
  END                               AS net_new_source_categories,
    CASE
      WHEN lead_source in ('Web Chat', 'Request - Public Sector', 'Request - Professional Services', 'Request - Contact', 'Email Request')
        THEN 'Inbound Request'
      WHEN lead_source in ('Gated Content - General', 'Gated Content - Report', 'Gated Content - Video', 'Gated Content - Whitepaper')
        THEN 'Gated Content'
      WHEN lead_source in ('Conference', 'Field Event', 'Live Event', 'Owned Event', 'Virtual Sponsorship')
        THEN 'Field Marketing'
      WHEN lead_source in ('Prospecting - LeadIQ', 'Prospecting - General', 'Prospecting', 'LinkedIn', 'Leadware', 'Datanyze', 'DiscoverOrg', 'Clearbit')
        THEN 'Prospecting'
      WHEN lead_source in ('Demo')
        THEN 'Demo'
      WHEN lead_source in ('Trial - Enterprise')
        THEN 'Trial - Self-managed'
      WHEN lead_source in ('Trial - GitLab.com')
        THEN 'Trial - SaaS (GitLab.com)'
      WHEN lead_source in ('SDR Generated')     
        THEN 'SDR Generated'
      WHEN lead_source in ('AE Generated')
        THEN 'AE Generated'
      WHEN lead_source in ('Webcast')
        THEN 'Webcast'
      WHEN lead_source in ('Web Direct', 'Web')
        THEN 'Web Direct'
      WHEN lead_source in ('Newsletter', 'Security Newsletter', 'Email Subscription')
        THEN 'Newsletter'
      WHEN lead_source in ('Employee Referral', 'External Referral')
        THEN 'Referral'
      WHEN lead_source in ('GitLab.com', 'Gitlab.com')
        THEN 'GitLab.com'
      WHEN lead_source in ('OSS', 'Education')
        THEN 'EDU/OSS'
      ELSE 'Other'
  END                               AS source_buckets,
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
