SELECT
		"Meeting_ID"          AS meeting_id,
		"Topic"               AS meeting_topic,
		"Day_of_The_Week"     AS day_of_the_week,
		"Call_Time"           AS call_time,
		"Call_Date"           AS call_date,
		"Duration_(hh:mm:ss)" AS length_of_call,
		"Participants"        AS count_of_participants
FROM "4733-ADD-ZOOM-TAKE-A-BREAK-DATA-TO-SHEETLOAD_RAW"."SHEETLOAD"."COMPANY_CALL_ATTENDANCE"
