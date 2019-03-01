{{ config(schema='analytics') }}

with actuals as (

	SELECT * FROM {{ref('sheetload_cmodashboard_actuals')}}

), goals as (

	SELECT * FROM {{ref('sheetload_cmodashboard_goals')}}

)

SELECT actuals.*,
		goals.sclau_per_dollar as goals_sclau_per_dollar,
		goals.sclau as goals_sclau,
		goals.cac as goals_cac,
		goals.sales_efficiency as goals_sales_efficiency,
		goals.contr_per_release as goals_contr_per_release,
		goals.twitter_mentions as goals_twitter_mentions,
		goals.unique_hosts as goals_unique_hosts,
		goals.active_users as goals_active_users,
		goals.downloads as goals_downloads
FROM actuals
LEFT JOIN goals
ON actuals.pk = goals.pk

