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


), greenhouse_finance_id AS (

    SELECT * 
    FROM {{ref('greenhouse_opening_custom_fields')}}
    WHERE opening_custom_field = 'finance_id'

), greenhouse_recruiting_xf AS (

    SELECT * 
    FROM {{ref('greenhouse_recruiting_xf')}}

), aggregated AS (

    SELECT 
      greenhouse_openings.job_id,
      greenhouse_finance_id.opening_custom_field_display_value AS finance_id,
      greenhouse_jobs.job_created_at, 
      greenhouse_jobs.job_status,
      greenhouse_openings.opening_id, 
      greenhouse_recruiting_xf.is_hired_in_bamboo,
      greenhouse_openings.target_start_date,
      greenhouse_openings.job_opened_at                         AS opening_date,
      greenhouse_openings.job_closed_at                         AS closing_date,
      greenhouse_openings.close_reason,
      greenhouse_jobs.job_name                                  AS job_title, 
      greenhouse_department.department_name
    FROM greenhouse_openings
    LEFT JOIN greenhouse_jobs
      ON greenhouse_openings.job_id = greenhouse_jobs.job_id 
    LEFT JOIN greenhouse_department 
      ON greenhouse_department.department_id = greenhouse_jobs.department_id
    LEFT JOIN greenhouse_finance_id 
      ON greenhouse_finance_id.opening_id = greenhouse_openings.job_opening_id  
    LEFT JOIN greenhouse_recruiting_xf
      ON greenhouse_openings.hired_application_id = greenhouse_recruiting_xf.application_id
    WHERE greenhouse_jobs.job_opened_at IS NOT NULL 
)

SELECT *
FROM aggregated