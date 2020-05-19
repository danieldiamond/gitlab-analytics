WITH source AS (

	SELECT *
	FROM {{ source('google_analytics_360', 'session_hit') }}

), renamed AS(

	SELECT
	  --Keys
	  visit_id::FLOAT                 AS visit_id, 
	  visitor_id::VARCHAR             AS visitor_id, 
	    
	  --Info
	  visit_start_time::TIMESTAMP_TZ  AS visit_start_time,
          hit_number::INTEGER             AS hit_number,
	  is_entrance::BOOLEAN            AS is_entrance,
	  is_exit::BOOLEAN                AS is_exit,
	  referer::VARCHAR                AS referer,
	  data_source::VARCHAR            AS data_source,
	  page_hostname::VARCHAR          AS host_name,
	  page_page_path::VARCHAR         AS page_path,
	  page_page_title::VARCHAR        AS page_title

	FROM source

)

SELECT *
FROM renamed
