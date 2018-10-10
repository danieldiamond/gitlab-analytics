WITH source AS (

	SELECT 
		  md5(month_of::varchar) as pk,
		  month_of::date,
		  support_sla,
		  support_csat,
		  mr_per_release,
		  uptime_gitlab,
		  performance_gitlab,
		  days_security_issue_fix
	FROM historical.vpedashboard_actuals
)

SELECT *
FROM source