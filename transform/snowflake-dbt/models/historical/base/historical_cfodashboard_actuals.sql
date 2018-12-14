WITH source AS (

	SELECT md5(month_of :: varchar)                    as pk,
				 month_of :: date,
				 nullif(days_to_close, '') :: float          as days_to_close,
				 nullif(days_sales_outstanding, '') :: float as days_sales_outstanding,
				 nullif(sales_efficiency_ratio, '') :: float as sales_efficiency_ratio
	FROM raw.historical.cfodashboard_actuals
)

SELECT *
FROM source
