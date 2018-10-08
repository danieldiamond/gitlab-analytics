WITH source AS (

	SELECT 
		  month_of::date,
		  "pipe-to-spend",
		  sclau,
		  cac,
		  sales_efficiency,
		  contr_per_release,
		  twitter_mentions,
		  unique_hosts,
		  active_users,
		  downloads
	FROM historical.cmodashboard_actuals
)

SELECT *
FROM source