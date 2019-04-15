{{ config({
    "schema": "analytics",
    "post-hook": "grant select on {{this}} to role reporter"
    })
}}

with actuals as (

	SELECT * FROM {{ref('sheetload_vpedashboard_actuals')}}

), goals as (

	SELECT * FROM {{ref('sheetload_vpedashboard_goals')}}

)

SELECT actuals.*,
		goals.support_sla as goal_support_sla,
		goals.support_csat as goal_support_csat,
		goals.mr_per_release as goal_mr_per_release,
		goals.uptime_gitlab as goal_uptime_gitlab,
		goals.performance_gitlab as goal_performance_gitlab,
		goals.days_security_issue_fix as goal_days_security_issue_fix
FROM actuals
LEFT JOIN goals
ON actuals.pk = goals.pk

