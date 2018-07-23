WITH source AS ( 

	SELECT *
	FROM sfdc.opportunity

),

    stages AS (
        SELECT * FROM {{ ref('sfdc_opportunitystage') }}
),

    renamed AS(

	SELECT 
		id as opportunity_id, 
		
		-- keys
		accountid as account_id,

		-- logistical info
		name as opportunity_name,
		opportunity_owner__c as owner,
		ownerid as owner_id,
		engagement_type__c as sales_path,
		sql_source__c as generated_source,
		COALESCE((initcap(sales_segmentation_o__c)), 'Unknown') as sales_segment,
		(initcap(ultimate_parent_sales_segment_o__c)) as parent_segment,
		type as sales_type,
		closedate as close_date,
		createddate as created_date,
		stagename as stage_name,
		sales_accepted_date__c as sales_accepted_date,

		-- opp info
		acv_2__c as acv,
		-- Should confirm which IACV is which
		incremental_acv_2__c as forecasted_iacv,
		incremental_acv__c as incremental_acv,
		renewal_amount__c as renewal_amount,
		renewal_acv__c as renewal_acv,
		nrv__c as nrv,
		sales_segmentation_o__c as segment,
		amount as total_contract_value,
		leadsource as lead_source,
		products_purchased__c AS products_purchased,
		reason_for_lost__c as reason_for_loss,
        reason_for_lost_details__c as reason_for_loss_details,
		CASE WHEN
            incremental_acv_2__c :: DECIMAL < 5000
            THEN '1 - Small (<5k)'
          WHEN incremental_acv_2__c :: DECIMAL >= 5000 AND incremental_acv_2__c :: DECIMAL < 25000
            THEN '2 - Medium (5k - 25k)'
          WHEN incremental_acv_2__c :: DECIMAL >= 25000 AND incremental_acv_2__c :: DECIMAL < 100000
            THEN '3 - Big (25k - 100k)'
          WHEN incremental_acv_2__c :: DECIMAL >= 100000
            THEN '4 - Jumbo (>100k)'
          ELSE '5 - Unknown' END                                          AS deal_size,
		
		
		CASE WHEN acv_2__c >= 0 THEN 1
             ELSE 0
        	END AS closed_deals,  -- so that you can exclude closed deals that had negative impact

		-- metadata
		isdeleted as is_deleted


	FROM source
	WHERE accountid IS NOT NULL
	AND isdeleted = FALSE

),

    layered AS (
        SELECT
            renamed.*,
            s.is_won AS is_won,
            s.stage_id as opportunity_stage_id
        FROM renamed
        INNER JOIN stages s on renamed.stage_name = s.primary_label
)

SELECT *
FROM layered
