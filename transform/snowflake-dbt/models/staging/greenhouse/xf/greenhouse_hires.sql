WITH applications AS (

    SELECT *,
      ROW_NUMBER() OVER (PARTITION BY candidate_id ORDER BY applied_at)     AS greenhouse_candidate_row_number
    FROM  {{ ref ('greenhouse_applications_source') }}
    WHERE application_status = 'hired'

), offers AS (

    SELECT * 
    FROM  {{ ref ('greenhouse_offers_source') }}
    WHERE offer_status = 'accepted'

), bamboo as (
 
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
      valid_from_date,
      hire_date,
      region,
      bamboo_row_number,
      is_rehire
    FROM bamboohr_mapping
    LEFT JOIN bamboo 
      ON bamboo.employee_id = bamboohr_mapping.employee_id
      AND (bamboo_row_number = 1 OR is_rehire=TRUE)

), final AS (

    SELECT 
      applications.application_id,  
      candidate_id, 
      employee_id,
      offers.start_date                                         AS candidate_target_hire_date, 
      region,
      greenhouse_candidate_row_number,
      bamboo_row_number,
      valid_from_date,
      hire_date,
      CASE WHEN hire_date <='2018-12-01' 
      --because we don't always have all job statuses we are using the candidate target hire date
            THEN candidate_target_hire_date
           WHEN candidate_target_hire_date > '2018-12-01' 
                AND bamboo_row_number = 1 
                AND greenhouse_candidate_row_number =1
            THEN hire_date
           ELSE candidate_target_hire_date END                 AS bamboo_hire_date,
      CASE WHEN bamboo_row_number=1 AND greenhouse_candidate_row_number = 1 
            THEN 'hire'
           WHEN is_rehire = TRUE 
            THEN 'rehire'
           WHEN valid_from_date IS NULL AND hire_date IS NOT NULL 
            THEN 'hire'
           WHEN greenhouse_candidate_row_number>1 
                AND hire_date IS NOT NULL 
            THEN 'transfer'
           WHEN greenhouse_candidate_row_number = 1 
                AND candidate_target_hire_date > bamboo_hire_date 
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