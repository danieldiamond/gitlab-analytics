With greenhouse_openings AS (

    SELECT * 
    FROM {{ref('greenhouse_openings')}}

), greenhouse_jobs AS (

    SELECT * 
    FROM {{ref('greenhouse_jobs')}}

), aggregated AS (

    SELECT 
      greenhouse_openings.job_id
      greenhouse_openings.opening_id, 
      greenhouse_jobs.job_name, 
      greenhouse_jobs.job_status, 
      greenhouse_jobs.job_opened_at, 
      greenhouse_jobs.job_closed_at 
    FROM greenhouse_openings
    LEFT JOIN greenhouse_jobs
      ON greenhouse_openings.job_id = greenhouse_jobs.job_id 
--WHERE openings.opening_id IS NULL

)

SELECT *
FROM aggregated