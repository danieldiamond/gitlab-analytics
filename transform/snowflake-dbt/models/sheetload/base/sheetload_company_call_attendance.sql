WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'company_call_attendance') }}

), renamed AS (

	    SELECT
	        "Call_Date"::DATE     										AS call_date,
	        "Topic"           											AS meeting_topic,
	        "Day_of_The_Week"     										AS day_of_the_week,
	        "Call_Time"           										AS call_time,
	        split_part("Duration_(hh:mm:ss)", ':', 2) 					AS length_of_call_mins,
	        "Participants"        										AS count_of_participants
		FROM source
)

SELECT *
FROM renamed
