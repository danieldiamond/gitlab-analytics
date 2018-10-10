WITH source AS (

	SELECT 
		  md5(month_of::varchar) as pk,
		  month_of::date,
		  days_to_close,
		  days_sales_outstanding,
		  sales_efficiency_ratio
	FROM historical.cfodashboard_actuals
)

SELECT *
FROM source