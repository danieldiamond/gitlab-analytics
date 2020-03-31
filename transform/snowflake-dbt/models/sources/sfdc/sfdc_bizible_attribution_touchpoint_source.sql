WITH source AS (

  SELECT *
  FROM {{ source('salesforce', 'bizible_attribution_touchpoint') }}



), renamed AS (

    SELECT
      id                                      AS touchpoint_id,
      -- sfdc object lookups
      bizible2__sf_campaign__c                AS campaign_id,
      bizible2__opportunity__c                AS opportunity_id,
      bizible2__contact__c                    AS bizible_contact,
      bizible2__account__c                    AS bizible_account,      
      
      -- attribution counts
      bizible2__count_first_touch__c          AS bizible_count_first_touch,
      bizible2__count_lead_creation_touch__c  AS bizible_count_lead_creation_touch,
      bizible2__count_custom_model__c         AS bizible_attribution_percent_full_path,
      bizible2__count_u_shaped__c             AS bizible_count_u_shaped,
      bizible2__count_w_shaped__c             AS bizible_count_w_shaped,

      -- touchpoint info
      bizible2__touchpoint_date__c            AS bizible_touchpoint_date,
      bizible2__touchpoint_position__c        AS bizible_touchpoint_position,
      bizible2__touchpoint_source__c          AS bizible_touchpoint_source,
      bizible2__touchpoint_type__c            AS bizible_touchpoint_type,      
      bizible2__ad_campaign_name__c           AS bizible_ad_campaign_name,
      bizible2__ad_group_name__c              AS bizible_ad_group_name,
      bizible2__form_url__c                   AS bizible_form_url,
      bizible2__landing_page__c               AS bizible_landing_page,
      bizible2__marketing_channel__c          AS bizible_marketing_channel,
      bizible2__marketing_channel_path__c     AS marketing_channel_path,
      bizible2__medium__c                     AS bizible_medium, 
      bizible2__referrer_page__c              AS bizible_referrer_page,   
      bizible2__sf_campaign__c                AS bizible_salesforce_campaign,  

      -- touchpoint revenue info
      bizible2__revenue_custom_model__c       AS bizible_revenue_full_path,
      bizible2__revenue_custom_model_2__c     AS bizible_revenue_custom_model,
      bizible2__revenue_first_touch__c        AS bizible_revenue_first_touch,
      bizible2__revenue_lead_conversion__c    AS bizible_revenue_lead_conversion,
      bizible2__revenue_u_shaped__c           AS bizible_revenue_u_shaped,
      bizible2__revenue_w_shaped__c           AS bizible_revenue_w_shaped,

      isdeleted::BOOLEAN                      AS is_deleted
    
    FROM source
)

SELECT *
FROM renamed
