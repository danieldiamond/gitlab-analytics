WITH applications AS (

    SELECT *
    FROM  {{ ref ('greenhouse_applications_source') }}
    WHERE applied_at >='2017-01-01'

), offers AS (

    SELECT * 
    FROM  {{ ref ('greenhouse_offers_source') }}

), greenhouse_application_jobs AS (

    SELECT *
    FROM  {{ ref ('greenhouse_applications_jobs_source') }}

), jobs AS (

    SELECT * 
    FROM  {{ ref ('greenhouse_jobs_source') }}


), job_req AS (

    SELECT 
      applications.application_id, 
      jobs.*
    FROM applications
    LEFT JOIN greenhouse_application_jobs 
      ON applications.application_id = greenhouse_application_jobs.application_id
    INNER JOIN jobs 
      ON greenhouse_application_jobs.job_id = jobs.job_id
      AND applications.applied_at BETWEEN jobs.job_created_at 
        AND COALESCE(jobs.job_closed_at, DATEADD(week,3,CURRENT_DATE()))
      AND jobs.job_id IS NOT NULL AND jobs.requisition_id IS NOT NULL
    
), greenhouse_departments AS (

    SELECT * 
    FROM  {{ ref ('greenhouse_departments_source') }}

), greenhouse_sources AS (

    SELECT * 
    FROM  {{ ref ('greenhouse_sources_source') }}

), greenhouse_sourcer AS (

    SELECT * 
    FROM {{ ref ('greenhouse_sourcer') }}

), candidates AS (

    SELECT * 
    FROM {{ ref ('greenhouse_candidates_source') }}

), rejection_reasons AS (
    
    SELECT * 
    FROM {{ ref ('greenhouse_rejection_reasons_source') }}

), cost_center AS (

    SELECT DISTINCT division, department
    FROM {{ref('cost_center_division_department_mapping')}}

), bamboo AS (

    SELECT greenhouse_candidate_id, hire_date 
    FROM {{ref('bamboohr_id_employee_number_mapping')}}
    WHERE greenhouse_candidate_id IS NOT NULL

), final AS (

    SELECT 
        applications.application_id, 
        offers.offer_id,
        applications.candidate_id, 
        job_req.job_id,
        job_req.requisition_id, 
        applications.application_status,
        applications.stage_name                                                        AS current_stage_name, 
        offers.offer_status,
        applications.applied_at                                                         AS application_date,
        offers.sent_at                                                                  AS offer_sent_date,
        offers.resolved_at                                                              AS offer_resolved_date,
        offers.start_date                                                               AS candidate_target_hire_date,
        applications.rejected_at                                                        AS rejected_date,
        job_req.job_name,
        greenhouse_departments.department_name                                          AS department_name,
        cost_center.division                                                            AS division,                                             
        CASE WHEN lower(greenhouse_departments.department_name) LIKE '%sales%' 
               THEN 'Sales'
             WHEN greenhouse_departments.department_name = 'Dev' 
               THEN 'Engineering'
             WHEN greenhouse_departments.department_name = 'Customer Success Management' 
               THEN 'Sales'
             ELSE COALESCE(cost_center.division, 
                    greenhouse_departments.department_name) END                         AS division_modified,     
        greenhouse_sources.source_name                                                  AS source_name,
        greenhouse_sources.source_type                                                  AS source_type,
        greenhouse_sourcer.sourcer_name,
        candidates.candidate_recruiter,
        candidates.candidate_coordinator,
        IFF(greenhouse_sources.source_name ='LinkedIn (Prospecting)',True, False)       AS sourced_candidate,
        rejection_reasons.rejection_reason_name,
        rejection_reasons.rejection_reason_type,
        job_req.job_status                                                              AS current_job_req_status,
        IFF(offers.offer_status ='accepted',
                DATEDIFF('day', applications.applied_at, offers.resolved_at),
                NULL)                                                                   AS time_to_offer,
        IFF(bamboo.hire_date IS NOT NULL, TRUE, FALSE)                                  AS is_hired_in_bamboo
    FROM applications 
    LEFT JOIN job_req
      ON job_req.application_id = applications.application_id
    LEFT JOIN  greenhouse_departments 
      ON job_req.department_id = greenhouse_departments.department_id
      AND job_req.organization_id = greenhouse_departments.organization_id
    LEFT JOIN greenhouse_sources 
      ON greenhouse_sources.source_id = applications.source_id 
    LEFT JOIN offers 
      ON applications.application_id = offers.application_id
    LEFT JOIN candidates
      ON applications.candidate_id = candidates.candidate_id  
    LEFT JOIN greenhouse_sourcer
      ON applications.application_id = greenhouse_sourcer.application_id
    LEFT JOIN rejection_reasons
      ON rejection_reasons.rejection_reason_id = applications.rejection_reason_id
    LEFT JOIN cost_center
      ON TRIM(greenhouse_departments.department_name)=TRIM(cost_center.department)
    LEFT JOIN bamboo
      ON bamboo.greenhouse_candidate_id = applications.candidate_id  

)

SELECT *
FROM final 


