WITH applications AS (

    SELECT *,
      ROW_NUMBER() OVER (PARTITION BY candidate_id ORDER BY applied_at)     AS greenhouse_candidate_row_number
    FROM  {{ ref ('greenhouse_applications_source') }}
    WHERE application_status = 'hired'

), offers AS (

    SELECT * 
    FROM  {{ ref ('greenhouse_offers_source') }}
    WHERE offer_status = 'accepted'

), bamboo_status as (
 
    SELECT *,
      ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY valid_from_date) AS bamboo_row_number
    FROM {{ ref ('bamboohr_employment_status_xf') }}
     
 ), bamboohr_mapping AS (
 
    SELECT *
    FROM {{ ref ('bamboohr_id_employee_number_mapping') }}
   
), bamboo_hires AS (

    SELECT 
      bamboohr_mapping.employee_id,
      bamboohr_mapping.greenhouse_candidate_id,
      bamboohr_mapping.hire_date,
      bamboo_status.valid_from_date,
      bamboohr_mapping.region,
      bamboo_status.bamboo_row_number,
      bamboo_status.is_rehire
    FROM bamboohr_mapping
    LEFT JOIN bamboo_status 
      ON bamboo_status.employee_id = bamboohr_mapping.employee_id
    WHERE bamboo_status.bamboo_row_number = 1 OR bamboo_status.is_rehire= 'True'

), final AS (

    SELECT 
      applications.application_id,  
      applications.candidate_id, 
      bamboo_hires.employee_id,
      offers.start_date                                             AS candidate_target_hire_date, 
      bamboo_hires.region,
      applications.greenhouse_candidate_row_number,
      bamboo_hires.bamboo_row_number,
      bamboo_hires.valid_from_date,
      bamboo_hires.hire_date,
      CASE WHEN candidate_target_hire_date IS NULL 
            THEN valid_from_date
           WHEN candidate_target_hire_date IS NOT NULL 
                AND applications.greenhouse_candidate_row_number =1 
                AND bamboo_hires.bamboo_row_number = 1 
                AND ABS(DATEDIFF(day,candidate_target_hire_date, bamboo_hires.hire_date))<120
            THEN hire_date
        ELSE candidate_target_hire_date END                     AS hire_date_mod,
      CASE WHEN bamboo_row_number=1 AND greenhouse_candidate_row_number = 1 
            THEN 'hire'
           WHEN is_rehire = TRUE 
            THEN 'rehire'
           WHEN greenhouse_candidate_row_number>1 
            THEN 'transfer'
           ELSE NULL END                                       AS hire_type
    FROM applications
    LEFT JOIN offers
      ON offers.application_id = applications.application_id
    LEFT JOIN bamboo_hires 
      ON bamboo_hires.greenhouse_candidate_id = applications.candidate_id
)    

SELECT 
  application_id,
  candidate_id,
  region,
  greenhouse_candidate_row_number,
  bamboo_hire_date,
  hire_type
FROM final 