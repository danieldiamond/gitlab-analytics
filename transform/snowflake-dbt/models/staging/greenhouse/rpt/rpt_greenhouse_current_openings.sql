WITH greenhouse_openings AS (

    SELECT * 
    FROM {{ref('greenhouse_openings_source')}}

), greenhouse_jobs AS (

    SELECT * 
    FROM {{ref('greenhouse_jobs_source')}}

), greenhouse_department AS (

    SELECT 
      department_id,
      department_name,
      CASE WHEN department_name LIKE '%Enterprise Sales%'
           THEN 'Enterprise Sales'
           WHEN department_name LIKE '%Commercial Sales%' 
           THEN 'Commercial Sales'
           ELSE department_name END AS department_modified
    FROM {{ref('greenhouse_departments_source')}}
      
), greenhouse_organization AS (

    SELECT * 
    FROM {{ref('greenhouse_organizations_source')}}


), greenhouse_finance_id AS (

    SELECT 
      opening_id, 
      opening_custom_field_display_value AS ghp_id
    FROM {{ref('greenhouse_opening_custom_fields_source')}}
    WHERE opening_custom_field = 'finance_id'

), greenhouse_hiring_manager AS (

    SELECT 
      opening_id, 
      opening_custom_field_display_value AS hiring_manager
    FROM {{ref('greenhouse_opening_custom_fields_source')}}
    WHERE opening_custom_field = 'hiring_manager'

), greenhouse_opening_type AS (
  
    SELECT
      opening_id, 
      opening_custom_field_display_value AS opening_type
    FROM {{ref('greenhouse_opening_custom_fields_source')}}
    WHERE opening_custom_field = 'type'
    
), greenhouse_recruiting_xf AS (

    SELECT * 
    FROM {{ref('greenhouse_recruiting_xf')}}

), division_mapping AS (
  
    SELECT DISTINCT 
      date_actual,   
      division, 
      department
    FROM {{ref('employee_directory_intermediate')}}

), cost_center_mapping AS (

    SELECT *
    FROM {{ref('cost_center_division_department_mapping')}}
 
), hires AS (
  
    SELECT *
    FROM {{ref('bamboohr_id_employee_number_mapping')}}

), aggregated AS (

    SELECT 
      greenhouse_openings.job_id,
      greenhouse_finance_id.ghp_id,
      greenhouse_jobs.job_created_at, 
      greenhouse_jobs.job_status,
      greenhouse_openings.opening_id, 
      greenhouse_recruiting_xf.is_hired_in_bamboo,
      greenhouse_openings.target_start_date,
      greenhouse_openings.job_opened_at                         AS opening_date,
      greenhouse_openings.job_closed_at                         AS closing_date,
      greenhouse_openings.close_reason,
      greenhouse_jobs.job_name                                  AS job_title, 
      greenhouse_department.department_name,
      COALESCE(division_mapping.division, cost_center_mapping.division) AS division,
      greenhouse_hiring_manager.hiring_manager,
      greenhouse_opening_type.opening_type,
      hires.employee_id,
      hires.first_name || ' ' || hires.last_name                AS hire_name
    FROM greenhouse_openings
    LEFT JOIN greenhouse_jobs
      ON greenhouse_openings.job_id = greenhouse_jobs.job_id 
    LEFT JOIN greenhouse_department 
      ON greenhouse_department.department_id = greenhouse_jobs.department_id
    LEFT JOIN greenhouse_finance_id 
      ON greenhouse_finance_id.opening_id = greenhouse_openings.job_opening_id  
    LEFT JOIN greenhouse_recruiting_xf
      ON greenhouse_openings.hired_application_id = greenhouse_recruiting_xf.application_id 
    LEFT JOIN greenhouse_hiring_manager
      ON greenhouse_hiring_manager.opening_id = greenhouse_openings.job_opening_id  
    LEFT JOIN greenhouse_opening_type
      ON greenhouse_opening_type.opening_id = greenhouse_openings.job_opening_id  
    LEFT JOIN division_mapping
      ON division_mapping.department = TRIM(greenhouse_department.department_modified)
      AND division_mapping.date_actual = DATE_TRUNC(DAY,greenhouse_openings.job_opened_at)
    LEFT JOIN cost_center_mapping
      ON cost_center_mapping.department = TRIM(greenhouse_department.department_modified)
    LEFT JOIN hires
      ON hires.greenhouse_candidate_id = greenhouse_recruiting_xf.candidate_id
    WHERE greenhouse_jobs.job_opened_at IS NOT NULL 
)

SELECT *
FROM aggregated