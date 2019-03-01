{{ config(schema='analytics') }}

with actuals as (

	SELECT * FROM {{ref('sheetload_cfodashboard_actuals')}}

), goals as (

	SELECT * FROM {{ref('sheetload_cfodashboard_goals')}}

)

SELECT actuals.*,
		goals.days_to_close as goals_days_to_close,
		goals.days_sales_outstanding as goals_days_sales_outstanding,
		goals.sales_efficiency_ratio as goals_sales_efficiency_ratio
FROM actuals
LEFT JOIN goals
ON actuals.pk = goals.pk

