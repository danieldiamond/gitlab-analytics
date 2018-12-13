with source as (

	SELECT *
	FROM historical.interviewers_and_scores 

), renamed as (

	SELECT interviewer,
			total::numeric as total_number_of_interviews,
			strong_no_hire::numeric as strong_no_hire,
			no_hire::numeric as no_hire,
			hire::numeric as hire,
			strong_hire::numeric as strong_hire,
			((hire::numeric + strong_hire::numeric)/total::numeric)*100 as positive_responses,
			((no_hire::numeric + strong_no_hire::numeric)/total::numeric)*100 as negative_responses,
			((split_part(avg_time_to_complete, 'hrs', 1))::numeric*60) +
			(split_part(split_part(avg_time_to_complete, 'hrs', 2), ' ', 1))::numeric 
				as avg_time_to_complete_in_minutes -- this comes calculated and there's no way to recalculate
	FROM source

)

SELECT *
FROM renamed