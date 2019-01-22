WITH source AS (

	SELECT *
	FROM raw.sheetload.sfdc_agent_mapping

), renamed AS (

	SELECT id as account_owner_id,
			name as account_owner_name
	FROM source

)

SELECT *
FROM renamed
