SELECT event_id as root_id, 
		web_page_id as id
FROM {{ref('unnested_events')}}