WITH source AS (

	SELECT 
		  month_of::date,
		  active_gitlab_installations,
		  active_users_aws,
		  active_users_gcp,
		  active_users_azure
	FROM historical.alliancesdashboard_actuals
)

SELECT *
FROM source