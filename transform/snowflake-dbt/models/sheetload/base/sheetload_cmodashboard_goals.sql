WITH source AS (

	SELECT md5(month_of :: varchar)               as pk,
				 month_of :: date 						as month_of,
				 nullif(sclau_per_dollar, '') :: float  as sclau_per_dollar,
				 nullif(sclau, '') :: float             as sclau,
				 nullif(cac, '') :: float               as cac,
				 nullif(sales_efficiency, '') :: float  as sales_efficiency,
				 nullif(contr_per_release, '') :: float as contr_per_release,
				 nullif(twitter_mentions, '') :: float  as twitter_mentions,
				 nullif(unique_hosts, '') :: float      as unique_hosts,
				 nullif(active_users, '') :: float      as active_users,
				 nullif(downloads, '') :: float         as downloads
	FROM raw.sheetload.cmodashboard_goals
)

SELECT *
FROM source
