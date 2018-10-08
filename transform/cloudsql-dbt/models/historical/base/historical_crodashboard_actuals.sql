WITH source AS (

	SELECT 
		  month_of::date,
		  ramped_reps_on_quota,
		  rep_productivity,
		  sales_efficiency  
	FROM historical.crodashboard_actuals
)

SELECT *
FROM source