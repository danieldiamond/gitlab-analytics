WITH events AS (

	 SELECT *
	 FROM {{ref('snowplow_unnested_events')}}

)

SELECT event_id 	AS root_id,
	   web_page_id 	AS id
FROM events
