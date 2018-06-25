WITH source AS (

	SELECT *
	FROM zuora.rateplan


), renamed AS(

	SELECT id as rate_plan_id,
		--keys	
		subscriptionid as subscription_id,
		productid as product_id
		-- info
		



	FROM source
	

)

SELECT *
FROM renamed