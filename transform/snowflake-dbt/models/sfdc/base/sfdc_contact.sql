WITH source AS (

    SELECT *
    FROM {{ source('salesforce', 'contact') }}



), renamed AS(

    SELECT
        -- id
        id                          AS contact_id,
        name                        AS contact_name,
        email                       AS contact_email,

        -- keys
        accountid                   AS account_id,
        masterrecordid              AS master_record_id,
        ownerid                     AS owner_id,
        recordtypeid                AS record_type_id,
        reportstoid                 AS reports_to_id, 

        --contact info
        
        title                       AS contact_title,
        role__c                     AS contact_role,
        mobilephone                 AS mobile_phone,
        person_score__c             AS person_score,

        department                  AS department,
        contact_status__c           AS contact_status,
        requested_contact__c        AS requested_contact,
        inactive_contact__c         AS inactive_contact,
        hasoptedoutofemail          AS has_opted_out_email,
        invalid_email_address__c    AS invalid_email_address,
        isemailbounced              AS email_is_bounced,
        emailbounceddate            AS email_bounced_date,
        emailbouncedreason          AS email_bounced_reason,

        mailingstreet               AS mailing_address,
        mailingcity                 AS mailing_city,
        mailingstate                AS mailing_state,
        mailingstatecode            AS mailing_state_code,
        mailingcountry              AS mailing_country,
        mailingcountrycode          AS mailing_country_code,
        mailingpostalcode           AS mailing_zip_code,

        -- info
        using_ce__c                 AS using_ce,
        ee_trial_start_date__c      AS ee_trial_start_date,
        ee_trial_end_date__c        AS ee_trial_end_date,
        industry__c                 AS industry,
        responded_to_githost_price_change__c AS responded_to_githost_price_change, -- maybe we can exclude this if it's not relevant
        leadsource                  AS lead_source,
        lead_source_type__c         AS lead_source_type,
        outreach_stage__c           AS outreach_stage,
        account_type__c             AS account_type,
        mql_timestamp__c            AS marketo_qualified_lead_timestamp,
        mql_datetime__c             AS marketo_qualified_lead_datetime,
        inquiry_datetime__c         AS inquiry_datetime,
        accepted_datetime__c        AS accepted_datetime,
        qualifying_datetime__c      AS qualifying_datetime,
        qualified_datetime__c       AS qualified_datetime,
        unqualified_datetime__c     AS unqualified_datetime,
        mkto_si__last_interesting_moment__c 
                                    AS marketo_last_interesting_moment,
        mkto_si__last_interesting_moment_date__c 
                                    AS marketo_last_interesting_moment_date,
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

        --gl info
        account_owner__c            AS account_owner,
        ae_comments__c              AS ae_comments,
        business_development_rep__c AS business_development_rep_name,
        outbound_bdr__c             AS outbound_business_development_rep_name,

        -- metadata
        createdbyid                 AS created_by_id,
        createddate                 AS created_date,
        lastactivitydate            AS last_activity_date,
        lastcurequestdate           AS last_cu_request_date,
        lastcuupdatedate            AS last_cu_update_date,
        lastmodifiedbyid            AS last_modified_by_id,
        lastmodifieddate            AS last_modified_date,
        systemmodstamp


    FROM source
    WHERE isdeleted = FALSE

)

SELECT *
FROM renamed
