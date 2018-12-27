with source as (
	SELECT *
	FROM raw.historical.hired_candidates 

), cleaned as (

	SELECT row_number() OVER (ORDER BY interview_date::timestamp),
			md5(UPPER(candidate_name)) as candidate_id,
			candidate_name,
			status,
			termination,
			trim(i_n.value::string) as interviewer_names,
			trim(nullif(i_s.value::string, '')) as interviewer_score,
			CASE WHEN interviewer_names LIKE '%,%' THEN True
				ELSE False END AS is_many_to_one_interview,
			candidate_origin,
			interview_date::date as interview_date,
			row_number() OVER (PARTITION BY candidate_name
	              ORDER BY interview_date ASC ) AS interview_step,
			candidate_owner_name,
			schedule as scheduler,
			posting_title, 
			posting_hiring_manager_name
	FROM source,
	lateral flatten(input=>split(interviewer_names, ',')) i_n,
	lateral flatten(input=>split(interview_score, ',')) i_s

)

SELECT *, 
		avg(interviewer_score::numeric) OVER (PARTITION BY candidate_name) AS candidate_average_score
FROM cleaned