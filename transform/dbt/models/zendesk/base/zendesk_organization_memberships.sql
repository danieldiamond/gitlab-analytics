WITH source AS (

	SELECT *
	FROM zendesk.organization_memberships

),

    renamed AS(

	SELECT
				 id        as organization_membership_id,
			-- keys
				 user_id,
				 organization_id,
			-- logistical info
				 "default" as is_default,
			-- metadata
				 url       as organization_membership_url,
				 created_at,
				 updated_at

    FROM source

)

SELECT *
FROM renamed