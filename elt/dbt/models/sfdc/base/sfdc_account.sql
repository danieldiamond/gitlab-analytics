WITH source AS (

	SELECT *
	FROM sfdc.account

), renamed AS(

	SELECT 
		id                      as account_id,
		name                    as account_name,
		-- keys
		{{this.schema}}.id15to18(
              substring(
                  regexp_replace(ultimate_parent_account__c,
                                '_HL_ENCODED_/|<a\s+href="/', '')
                  , 1
                  , 15)
          )                     as ultimate_parent_account_id,

		-- info
		sales_segmentation__c   as segment,
		industry,
		type                    as customer_type


		-- metadata
		


	FROM source
	WHERE id IS NOT NULL
	AND isdeleted = FALSE

)

SELECT *
FROM renamed