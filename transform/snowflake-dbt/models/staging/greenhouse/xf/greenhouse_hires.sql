WITH applications AS (

    SELECT *,
      ROW_NUMBER() OVER (PARTITION BY candidate_id ORDER BY applied_at) AS greenhouse_candidate_row_number
    FROM  {{ ref ('greenhouse_applications_source') }}
    WHERE application_status = 'hired'

), offers AS (

    SELECT * 
    FROM  {{ ref ('greenhouse_offers_source') }}
    WHERE offer_status = 'accepted'

), bamboo as (
 
    SELECT *,
      ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY valid_from_date) AS bamboo_row_number
    FROM "ANALYTICS"."ANALYTICS_SENSITIVE"."BAMBOOHR_EMPLOYMENT_STATUS_XF" 
 
 ), bamboohr_mapping AS (
 
    SELECT *
    FROM "ANALYTICS"."ANALYTICS_SENSITIVE"."BAMBOOHR_ID_EMPLOYEE_NUMBER_MAPPING"
   
), bamboo_hires AS (

    SELECT 
      bamboohr_mapping.employee_id,
      greenhouse_candidate_id,
      valid_from_date,
      hire_date,
      region,
      bamboo_row_number,
      is_rehire
    FROM bamboohr_mapping
    LEFT JOIN bamboo 
      ON bamboo.employee_id = bamboohr_mapping.employee_id
      AND (bamboo_row_number = 1 or is_rehire=TRUE)
), final AS (

    SELECT 
      applications.application_id,  
      candidate_id, 
      employee_id,
      offers.start_date AS candidate_target_hire_date, 
      region,
      greenhouse_candidate_row_number,
      bamboo_row_number,
      valid_from_date,
      hire_date,
      IFF(bamboo_row_number = 1 AND greenhouse_candidate_row_number =1, hire_date, candidate_target_hire_date) AS bamboo_hire_date,
      CASE WHEN bamboo_row_number=1 AND greenhouse_candidate_row_number = 1 
                THEN 'hire'
            WHEN is_rehire = TRUE 
                THEN 'rehire'
            WHEN valid_from_date IS NULL AND hire_date IS NOT NULL 
                THEN 'hire'
            WHEN greenhouse_candidate_row_number>1 AND hire_date IS NOT NULL 
                THEN 'transfer'
            WHEN greenhouse_candidate_row_number = 1 AND candidate_target_hire_date > bamboo_hire_date 
                THEN 'transfer'
            ELSE NULL END                                                                         AS hire_type
    FROM applications
    LEFT JOIN offers
      ON offers.application_id = applications.application_id
    LEFT JOIN bamboo_hires 
      ON bamboo_hires.greenhouse_candidate_id = applications.candidate_id
)    


