{% set year_value = env_var('CURRENT_YEAR') %}
{% set month_value = env_var('CURRENT_MONTH') %}

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