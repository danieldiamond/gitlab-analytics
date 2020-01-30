With greenhouse_openings AS (

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
      greenhouse_openings.opening_id, 
      greenhouse_jobs.job_name                          AS job_title, 
      greenhouse_jobs.job_opened_at, 
      greenhouse_organization.organization_name
    FROM greenhouse_openings
    LEFT JOIN greenhouse_jobs
      ON greenhouse_openings.job_id = greenhouse_jobs.job_id 
    LEFT JOIN greenhouse_department 
      ON greenhouse_department.department_id = greenhouse_jobs.department_id
    LEFT JOIN greenhouse_organization
      ON greenhouse_organization.organization_id = greenhouse_jobs.organization_id
    WHERE greenhouse_jobs.job_closed_at IS NULL
      AND greenhouse_jobs.job_opened_at IS NOT NULL 
)

SELECT *
FROM aggregated