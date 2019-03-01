{{ config(schema='analytics') }}

with actuals as (

	SELECT * FROM {{ref('sheetload_alliancesdashboard_actuals')}}

), goals as (

	SELECT * FROM {{ref('sheetload_alliancesdashboard_goals')}}

)

SELECT actuals.*,
		goals.active_gitlab_installations as goal_active_gitlab_installations,
		goals.active_users_aws as goal_active_users_aws,
		goals.active_users_gcp as goal_active_users_gcp,
		goals.active_users_azure as goal_active_users_azure,
		goals.active_users_unknown as goal_active_users_unknown
FROM actuals
LEFT JOIN goals
ON actuals.pk = goals.pk