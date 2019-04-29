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
					CASE  WHEN lower(rate_plan_name) LIKE 'githost%' THEN 'GitHost'
								WHEN rate_plan_name IN ('#movingtogitlab', 'File Locking', 'Payment Gateway Test', 'Time Tracking', 'Training Workshop') THEN 'Other'
								WHEN lower(rate_plan_name) LIKE 'gitlab geo%' THEN 'Other'
								WHEN lower(rate_plan_name) LIKE 'basic%' THEN 'Basic'
								WHEN lower(rate_plan_name) LIKE 'bronze%' THEN 'Bronze'
								WHEN lower(rate_plan_name) LIKE 'ci runner%' THEN 'Other'
								WHEN lower(rate_plan_name) LIKE 'discount%' THEN 'Other'
								WHEN lower(rate_plan_name) LIKE '%premium%' THEN 'Premium'
								WHEN lower(rate_plan_name) LIKE '%starter%' THEN 'Starter'
								WHEN lower(rate_plan_name) LIKE '%ultimate%' THEN 'Ultimate'
								WHEN lower(rate_plan_name) LIKE 'gitlab enterprise edition%' THEN 'Starter'
								WHEN rate_plan_name IN ('GitLab Service Package', 'Implementation Services Quick Start', 'Implementation Support', 'Support Package') THEN 'Support'
								WHEN lower(rate_plan_name) LIKE 'gold%' THEN 'Gold'
								WHEN rate_plan_name = 'Pivotal Cloud Foundry Tile for GitLab EE' THEN 'Starter'
								WHEN lower(rate_plan_name) LIKE 'plus%' THEN 'Plus'
								WHEN lower(rate_plan_name) LIKE 'premium%' THEN 'Premium'
								WHEN lower(rate_plan_name) LIKE 'silver%' THEN 'Silver'
								WHEN lower(rate_plan_name) LIKE 'standard%' THEN 'Standard'
								WHEN rate_plan_name = 'Trueup' THEN 'Trueup'
					ELSE 'Other' END AS product_category
			FROM renamed
)

SELECT *
FROM with_product_category
