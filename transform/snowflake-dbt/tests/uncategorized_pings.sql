with live_data as (

	SELECT distinct ping_name
	FROM {{ref('pings_list')}}

), category_mapping as (

	SELECT distinct stats_used_key_name
	FROM {{ref('ping_metrics_to_stage_mapping_data')}}

)

SELECT *
FROM live_data
LEFT JOIN category_mapping
ON live_data.ping_name = category_mapping.stats_used_key_name
WHERE category_mapping.stats_used_key_name IS NULL
AND category_mapping.stats_used_key_name NOT IN ('failed_deployments', 'operations_dashboard.users_with_projects_added',
'projects_enforcing_code_owner_approval', 'operations_dashboard.default_dashboard', 
'successful_deployments', 'projects_with_error_tracking_enabled')