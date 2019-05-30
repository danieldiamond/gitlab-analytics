WITH source AS (

  SELECT *
  FROM {{ source('salesforce', 'bizible_attribution_touchpoint') }}



), renamed AS (

    SELECT
      id                                    AS touchpoint_id,
      bizible2__sf_campaign__c              AS campaign_id,
      bizible2__opportunity__c              AS opportunity_id,

      bizible2__marketing_channel__c        AS marketing_channel,
      bizible2__marketing_channel_path__c   AS marketing_channel_path,
      bizible2__count_custom_model__c       AS attribution_percent_full_path,
      bizible2__touchpoint_source__c        AS touchpoint_source,
      bizible2__medium__c                   AS medium


    FROM source
  WHERE isdeleted = FALSE

)

SELECT *
FROM renamed
