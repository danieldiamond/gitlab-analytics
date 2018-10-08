WITH source AS (

	SELECT 
		  month_of::date,
		  ramped_reps_on_quota
	FROM historical.crodashboard_goals
)

SELECT *
FROM source