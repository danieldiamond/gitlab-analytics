WITH source AS (

	SELECT *
	FROM sfdc.bizible2__bizible_attribution_touchpoint__c

), renamed AS (

    SELECT
      id                                    as touchpoint_id,
      bizible2__sf_campaign__c              as campaign_id,
      bizible2__opportunity__c              as opportunity_id,

      bizible2__marketing_channel__c        as marketing_channel,
      bizible2__marketing_channel_path__c   as marketing_channel_path,
      bizible2__count_custom_model__c       as attribution_percent_full_path,
      bizible2__touchpoint_source__c        as touchpoint_source,
      bizible2__medium__c                   as medium
    FROM source

)

SELECT *
FROM renamed