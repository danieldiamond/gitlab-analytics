with source as (
	SELECT *
	FROM raw.historical.hired_candidates 
), cleaned as (

SELECT row_number() OVER (),
		md5(UPPER(candidate_name)) as candidate_id,
		candidate_name,
		status,
		termination,
		{{ unnest('interviewer_names') }} as interviewer_names,
		{{ unnest('interview_score') }} as interviewer_score,
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
FROM source

)

SELECT *, 
		avg(interviewer_score::numeric) OVER (PARTITION BY candidate_name) AS candidate_average_score
FROM cleaned