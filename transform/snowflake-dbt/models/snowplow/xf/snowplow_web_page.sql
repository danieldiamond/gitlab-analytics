SELECT event_id as root_id, 
		web_page_id as id
FROM {{ref('snowplow_unnested_events')}}