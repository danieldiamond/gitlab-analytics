with source as (

	SELECT *
	FROM {{ var("database") }}.tap_postgres.version_db_version_checks

), renamed as (

	SELECT created_at,
			gitlab_version,
			host_id,
			id,
			referer_url,
			request_data,
			updated_at
	FROM source
)

SELECT * 
FROM renamed
WHERE created_at >= '2019-01-20'
