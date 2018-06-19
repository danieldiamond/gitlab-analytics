WITH source AS (

	SELECT *
	FROM sfdc.opportunity

), renamed AS(

	SELECT 
		id as opportunity_id, 
		
		-- keys
		accountid as account_id,

		-- info
		opportunity_owner__c as owner,
		engagement_type__c as sales_path,
		type as sales_type,
		acv_2__c as acv,
		sales_segmentation_o__c as segment,
		stagename as stage_name,
		closedate as close_date,
		CASE WHEN acv_2__c >= 0 THEN 1
             ELSE 0 --exclude closed deals that had negative impact
        END AS closed_deals,

		-- metadata
		isdeleted as is_deleted


	FROM source

)

SELECT *
FROM renamed