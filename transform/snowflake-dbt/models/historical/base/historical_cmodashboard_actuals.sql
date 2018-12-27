WITH source AS (

	SELECT md5(month_of :: varchar)               as pk,
				 month_of :: date                       as month_of,
				 nullif("pipe-to-spend", '') :: float   as pipe_to_spend,
				 nullif(sclau, '') :: float             as sclau,
				 nullif(cac, '') :: float               as cac,
				 nullif(sales_efficiency, '') :: float  as sales_efficiency,
				 nullif(contr_per_release, '') :: float as contr_per_release,
				 nullif(twitter_mentions, '') :: float  as twitter_mentions,
				 nullif(unique_hosts, '') :: float      as unique_hosts,
				 nullif(active_users, '') :: float      as active_users,
				 nullif(downloads, '') :: float         as downloads
	FROM raw.historical.cmodashboard_actuals
)

SELECT *
FROM source
