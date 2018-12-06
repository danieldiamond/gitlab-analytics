WITH source AS (

	SELECT *
	FROM sfdc.executive_business_review__c

), renamed AS(

	SELECT
		 id                            as ebr_id,
		 name                          as ebr_name,
		 ebr_date__c :: date           as ebr_date,
		--keys
		 ebr_account__c                as account_id,
		 ownerid                       as owner_id,
		--info
		 ebr_quarter__c                as ebr_quarter,
		 ebr_number__c                 as ebr_number,
		 ebr_outcome__c                as ebr_outcome,
		 ebr_summary__c                as ebr_summary,
		 ebr_status__c                 as ebr_status,
		 ebr_notes__c                  as ebr_notes,
		 first_date_success_updated__c as first_date_success_updated,
		 ebr_action_items_takeaways__c as ebr_action_items_takeaways,
		 ebr_success__c                as ebr_success,
		--metadata
		 lastmodifiedbyid              as last_modified_by_id,
		 createdbyid                   as created_by_id

	FROM source

	WHERE isdeleted IS FALSE

)

SELECT *
FROM renamed
