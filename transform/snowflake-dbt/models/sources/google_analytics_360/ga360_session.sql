WITH source AS (

	SELECT *
	FROM {{ source('google_analytics_360', 'ga_session') }}

), renamed AS(

	SELECT
	  --Keys
	  visit_id::FLOAT                 AS visit_id, 
	  visitor_id::VARCHAR             AS visitor_id, 
	    
	  --Info
	  date::DATE                      AS session_date,
	  visit_start_time::TIMESTAMP_TZ  AS visit_start_time,
      visit_number::FLOAT             AS visit_number,
      total_visits::FLOAT             AS total_visits,
      total_pageviews::FLOAT          AS total_pageviews,
      total_screenviews::FLOAT        AS total_screenviews,
      total_unique_screenviews::FLOAT AS total_unique_screenviews,
      total_hits::FLOAT               AS total_hits,
      total_new_visits::FLOAT         AS total_new_visits,
      total_time_on_screen::FLOAT     AS total_time_on_screen,
      total_time_on_site::FLOAT       AS total_time_on_site

	FROM source

)

SELECT *
FROM renamed
