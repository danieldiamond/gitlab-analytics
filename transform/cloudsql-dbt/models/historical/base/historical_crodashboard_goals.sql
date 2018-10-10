WITH source AS (

	SELECT 
		  md5(month_of::varchar) as pk,
		  month_of::date,
		  ramped_reps_on_quota
	FROM historical.crodashboard_goals
)

SELECT *
FROM source