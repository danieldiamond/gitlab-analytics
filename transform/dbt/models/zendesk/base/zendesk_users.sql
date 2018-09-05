WITH source AS (

	SELECT *
	FROM zendesk.users

),

    renamed AS(

	SELECT
		id as user_id, 
		email,
		name,

		-- keys
		custom_role_id,
		external_id,
		organization_id,
		default_group_id,

		-- logistical info
		alias,
		moderator as is_moderator,
		chat_only as is_chat_only,
		role_type,
		role,
		details,
		locale,
		notes,
		only_private_comments as has_only_private_comments,
		restricted_agent as is_restricted_agent,
		shared as is_shared,
		shared_agent as is_shared_agent,
		tags, 
		ticket_restriction,
		time_zone,		
		verified as is_verified,
		user_fields ->> 'agent_ooo' as is_out_of_office,

    	-- metadata
    	active as is_active,
    	created_at,
    	last_login_at,
    	suspended as is_suspended,
    	updated_at,
    	url as user_url

    FROM source

)

SELECT *
FROM renamed