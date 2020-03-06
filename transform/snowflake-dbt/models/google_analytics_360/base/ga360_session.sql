WITH source AS (

	SELECT *
	FROM {{ source('google_analytics_360','ga_session')}}

), renamed AS(

	SELECT
	    --Keys
	    visit_id::FLOAT					AS visit_id, 
	    visitor_id::VARCHAR				AS visitor_id, 
	    
	    --Info
	    date::DATE						AS session_date,
	    visit_start_time::TIMESTAMP_TZ	AS visit_start_time
	FROM source

)

SELECT *
FROM renamed
