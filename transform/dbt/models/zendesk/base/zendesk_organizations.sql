WITH source AS (

	SELECT *
	FROM zendesk.organizations

),

    renamed AS(

	SELECT
		id as organization_id,
		name as organization_name,

		-- keys
		external_id,
		group_id,
		organization_fields ->> 'salesforce_id' as sfdc_account_id,

		-- logistical info
		domain_names,
		details,
		notes,
		shared_tickets as has_shared_tickets,
		shared_comments as has_shared_comments,
		tags,
		organization_fields ->> 'account_owner' as account_owner,
		organization_fields ->> 'number_of_seats' as number_of_seats,
		organization_fields ->> 'market_segment' as market_segment,
		organization_fields ->> 'aar' as aar,
		organization_fields ->> 'support_level' as support_level,
		organization_fields ->> 'technical_account_manager' as technical_account_manager,

    	-- metadata
    	url as organization_url,
    	created_at,
    	updated_at

    FROM source

)

SELECT *
FROM renamed
