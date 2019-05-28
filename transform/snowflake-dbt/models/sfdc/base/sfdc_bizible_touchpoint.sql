{{ config({
  "schema": "analytics",
  "post-hook": "grant select on {{this}} to role reporter"
  })
}}

WITH source AS (

  SELECT *
  FROM {{ var("database") }}.salesforce_stitch.bizible2__bizible_touchpoint__c

), renamed AS (

    SELECT
      id                                      AS bizible_touchpoint_id,
      bizible2__bizible_person__c             AS bizible_person_id,
      bizible2__marketing_channel__c          AS bizible_marketing_channel,
      bizible2__marketing_channel_path__c     AS bizible_marketing_channel_path,
      bizible2__count_first_touch__c          AS bizible_attribution_percent_first_touch,
      bizible2__count_lead_creation_touch__c  AS bizible_attribution_percent_lead_creation_touch,
      bizible2__touchpoint_source__c          AS bizible_touchpoint_source,
      bizible2__touchpoint_date__c            AS bizible_touchpoint_date,
      bizible2__touchpoint_type__c            AS bizible_touchpoint_type,
      bizible2__landing_page__c               AS bizible_landing_page,
      bizible2__ad_campaign_name__c           AS bizible_ad_campaign_name,
      bizible2__medium__c                     AS bizible_medium


    FROM source
    WHERE isdeleted = FALSE

)

SELECT *
FROM renamed
