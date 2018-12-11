with interviewing_data_hired_candidates as (

	SELECT *
	FROM {{ref('interviewing_data_hired_candidates')}}

), interview_data_hired_candidates_with_performance as (

	SELECT *
	FROM {{ref('interviewing_data_hired_candidates_with_performance')}}

), candidates as (

	SELECT candidate_id, 
			termination, 
			received_positive_compensation_change, 
			received_any_bonus,
	       CASE WHEN received_positive_compensation_change IS NOT NULL THEN 'Compensation Improvement/Promotion'
	           WHEN received_any_bonus IS NOT NULL THEN 'Bonus'
	           WHEN status = 'Terminated' THEN 'Terminated' END AS criteria
	FROM interview_data_hired_candidates_with_performance
	WHERE (received_positive_compensation_change IS NOT NULL OR received_any_bonus IS NOT NULL OR status = 'Terminated')

), interviewers as (

	SELECT candidate_id, 
			interviewer_names, 
			interviewer_score, 
			is_many_to_one_interview
	FROM interviewing_data_hired_candidates

)

SELECT interviewers.*, candidates.criteria
FROM candidates
LEFT JOIN interviewers
    ON candidates.candidate_id = interviewers.candidate_id