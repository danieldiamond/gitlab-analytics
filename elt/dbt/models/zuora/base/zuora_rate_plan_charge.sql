WITH source AS (

	SELECT *
	FROM zuora.rateplancharge


), renamed AS(

	SELECT id as rate_plan_charge_id,
		--keys	
		originalid as original_id,
		rateplanid as rate_plan_id,

		-- info
		effectivestartdate as effective_start_date,
		effectiveenddate as effective_end_date,

		mrr


	FROM source
	

)

SELECT *
FROM renamed