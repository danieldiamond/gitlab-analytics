WITH source AS (

	SELECT 
		  md5(month_of::varchar) as pk,
		  active_gitlab_installations,
		  active_users_aws,
		  active_users_gcp,
		  active_users_azure,
		  active_users_unknown
	FROM historical.alliancesdashboard_goals
)

SELECT *
FROM source