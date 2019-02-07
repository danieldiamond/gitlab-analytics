WITH source AS (

	SELECT *
	FROM raw.gitlab_dotcom.licenses

), renamed AS (

    SELECT

      id :: integer                                 as license_id,
      created_at :: timestamp                       as license_created_at,
      updated_at :: timestamp                       as license_updated_at

    FROM source


)

SELECT *
FROM renamed