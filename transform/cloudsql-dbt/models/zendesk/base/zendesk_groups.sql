WITH source AS (

	SELECT *
	FROM zendesk.groups

),

    renamed AS(

	SELECT 
		 id  as group_id,
		 name as group_name,
		-- metadata
		 url AS group_url,
		 created_at,
		 updated_at,
		 deleted as is_deleted

    FROM source

)

SELECT *
FROM renamed