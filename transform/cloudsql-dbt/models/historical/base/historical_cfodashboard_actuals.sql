WITH source AS (

	SELECT 
		  month_of::date,
		  days_to_close,
		  days_sales_outstanding,
		  sales_efficiency_ratio
	FROM historical.cfodashboard_actuals
)

SELECT *
FROM source