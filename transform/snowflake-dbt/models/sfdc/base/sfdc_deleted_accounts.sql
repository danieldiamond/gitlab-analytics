with source as (

	SELECT *
	FROM {{ var("database") }}.salesforce_stitch.account

), renamed as (

	SELECT id as sfdc_account_id, 
			MASTERRECORDID as sfdc_master_record_id
	FROM source
	WHERE id IS NOT NULL
	AND isdeleted = TRUE

)

SELECT *
FROM renamed
