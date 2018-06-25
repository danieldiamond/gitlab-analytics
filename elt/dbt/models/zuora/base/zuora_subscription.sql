WITH source AS (

	SELECT *
	FROM zuora.subscription


), renamed AS(

	SELECT id as subscription_id,
		--keys	
		accountid as account_id,
		
		-- info
		status as subscription_status



	FROM source
	WHERE (excludefromanalysis__c = FALSE OR excludefromanalysis__c IS NULL)

)

SELECT *
FROM renamed