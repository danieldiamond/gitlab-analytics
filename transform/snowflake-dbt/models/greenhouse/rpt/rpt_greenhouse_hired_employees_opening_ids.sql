With employees AS (

    SELECT * 
    FROM {{ref('bamboohr_id_employee_number_mapping')}}

), applications AS (

    SELECT * 
    FROM {{ref('greenhouse_applications')}}

), openings AS (

    SELECT * 
    FROM {{ref('greenhouse_openings')}}

), jobs AS (

    SELECT * 
    FROM {{ref('greenhouse_jobs')}}

 ), bamboohr_job_info AS (

  SELECT 
    job_info.*,
    ROW_NUMBER() OVER(PARTITION BY employee_id order by effective_date)         AS job_row_number
  FROM "ANALYTICS"."ANALYTICS_SENSITIVE"."BAMBOOHR_JOB_INFO"  job_info

), aggregated AS (

    SELECT
        opening_id,
        job_name                                                                AS job_opening_name,
        greenhouse_jobs.job_opened_at, 
        CONCAT(first_name,' ', last_name)                                       AS full_name,     
        department                                                              AS department_hired_into, 
        division                                                                AS division_hired_into, 
        job_title                                                               AS job_hired_into     
    FROM employees
    INNER JOIN applications
      ON employees.greenhouse_candidate_id = applications.candidate_id 
    INNER JOIN openings
      ON openings.hired_application_id = applications.application_id
    INNER JOIN jobs 
      ON jobs.job_id = openings.job_id
    INNER JOIN bamboohr_job_info 
      ON bamboohr_job_info.employee_id = employees.employee_id 
      AND bamboohr_job_info.job_row_number = 1 --to get the initial job
    WHERE greenhouse_candidate_id IS NOT NULL 

)

SELECT * 
FROM aggregated