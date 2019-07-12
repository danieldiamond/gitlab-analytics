with live_data as (

	SELECT distinct full_ping_name
	FROM {{ref('pings_list')}}

), category_mapping as (

	SELECT distinct stats_used_key_name
	FROM {{ref('ping_metrics_to_stage_mapping_data')}}

)

SELECT *
FROM live_data
FULL OUTER JOIN category_mapping
  ON live_data.full_ping_name = category_mapping.stats_used_key_name
WHERE (category_mapping.stats_used_key_name IS NULL
  OR live_data.full_ping_name IS NULL)
