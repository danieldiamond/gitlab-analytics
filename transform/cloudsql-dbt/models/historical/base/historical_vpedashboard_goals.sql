WITH source AS (

	SELECT 
		  md5(month_of::varchar) as pk,
		  month_of::date,
		  nullif(support_sla, '')::float as support_sla,
		  nullif(support_csat, '')::float as support_csat,
		  nullif(mr_per_release, '')::float as mr_per_release,
		  nullif(uptime_gitlab, '')::float as uptime_gitlab,
		  nullif(performance_gitlab, '')::float as performance_gitlab,
		  nullif(days_security_issue_fix, '')::float as days_security_issue_fix
	FROM historical.vpedashboard_goals
)

SELECT *
FROM source