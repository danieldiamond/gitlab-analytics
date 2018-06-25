WITH source AS ( 

	SELECT *
	FROM sfdc.opportunity

), renamed AS(

	SELECT 
		id as opportunity_id, 
		
		-- keys
		accountid as account_id,

		-- l;ogistical info
		name as opportunity_name,
		opportunity_owner__c as owner,
		engagement_type__c as sales_path,
		sql_source__c as generated_source,
		COALESCE((initcap(sales_segmentation_o__c)), 'Unknown') as sales_segment,
		(initcap(ultimate_parent_sales_segment_o__c)) as parent_segment,
		type as sales_type,
		closedate as close_date,
		stagename as stage_name,
		sales_accepted_date__c as sales_accepted_date,

		-- opp info
		acv_2__c as acv,
		incremental_acv__c as incremental_acv,
		sales_segmentation_o__c as segment,
		amount as total_contract_value,
		leadsource as lead_source,
		
		
		CASE WHEN acv_2__c >= 0 THEN 1
             ELSE 0
        	END AS closed_deals,  -- so that you can exclude closed deals that had negative impact

		-- metadata
		isdeleted as is_deleted


	FROM source
	WHERE accountid IS NOT NULL

)

SELECT *
FROM renamed