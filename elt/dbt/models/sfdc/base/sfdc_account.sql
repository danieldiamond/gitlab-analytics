WITH source AS (

	SELECT *
	FROM sfdc.account

), renamed AS(

	SELECT 
		id as account_id, 
		name as account_name,
		-- keys
		

		-- info
		sales_segmentation__c as segment

		-- metadata
		


	FROM source
	WHERE id IS NOT NULL

)

SELECT *
FROM renamed