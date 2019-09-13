{% set year_value = var('year', run_started_at.strftime('%Y')) %}
{% set month_value = var('month', run_started_at.strftime('%m')) %}

{{config({
    "schema":"snowplow_" + year_value|string + '_' + month_value|string, 
  })
}}


WITH events AS (

	 SELECT *
	 FROM {{ref('snowplow_unnested_events')}}

)

SELECT event_id 	AS root_id,
	   web_page_id 	AS id
FROM events