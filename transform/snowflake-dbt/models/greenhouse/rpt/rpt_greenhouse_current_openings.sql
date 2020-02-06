WITH greenhouse_openings AS (

    SELECT * 
    FROM {{ref('greenhouse_openings')}}

), greenhouse_jobs AS (

    SELECT * 
    FROM {{ref('greenhouse_jobs')}}

), greenhouse_department AS (

    SELECT * 
    FROM {{ref('greenhouse_departments')}}
      
), greenhouse_organization AS (

    SELECT * 
    FROM {{ref('greenhouse_organizations')}}

), aggregated AS (

    SELECT 
      greenhouse_openings.job_id,
      greenhouse_jobs.job_created_at, 
      greenhouse_jobs.job_status,
      greenhouse_openings.opening_id, 
      greenhouse_openings.target_start_date,
      greenhouse_openings.job_opened_at as opening_date,
      greenhouse_openings.job_closed_at as closing_date,
      greenhouse_jobs.job_name                          AS job_title, 
      greenhouse_department.department_name
    FROM greenhouse_openings
    LEFT JOIN greenhouse_jobs
      ON greenhouse_openings.job_id = greenhouse_jobs.job_id 
    LEFT JOIN greenhouse_department 
      ON greenhouse_department.department_id = greenhouse_jobs.department_id
    WHERE greenhouse_jobs.job_closed_at IS NULL
      AND greenhouse_jobs.job_opened_at IS NOT NULL 
)

SELECT *
FROM aggregated