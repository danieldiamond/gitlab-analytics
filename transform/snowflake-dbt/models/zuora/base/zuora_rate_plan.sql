WITH source AS (

	SELECT *
	FROM {{ var("database") }}.zuora_stitch.rateplan

), renamed AS(

	SELECT
			  id                  as rate_plan_id,
		    name                as rate_plan_name,
				--keys
				subscriptionid      as subscription_id,
				productid           as product_id,
				productrateplanid   as product_rate_plan_id,
				-- info
				amendmentid         as amendement_id,
				amendmenttype       as amendement_type,

				--metadata
				updatedbyid         as updated_by_id,
				updateddate         as updated_date,
				createdbyid         as created_by_id,
				createddate         as created_date

			FROM source
			WHERE deleted = FALSE

), with_product_category as (

			SELECT *,
			{{product_category('rate_plan_name')}}
			FROM renamed
)

SELECT *
FROM with_product_category
