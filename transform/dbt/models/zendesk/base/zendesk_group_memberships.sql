WITH source AS (

	SELECT *
	FROM zendesk.group_memberships

),

    renamed AS(

	SELECT
		id as group_membership_id, 

		-- keys
		user_id,
		group_id,

		-- logistical info
		"default" as is_default,

    	-- metadata
    	url AS group_membership_url,   
		created_at,
		updated_at

    FROM source

)

SELECT *
FROM renamed