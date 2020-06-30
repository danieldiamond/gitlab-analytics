WITH offers AS (
  
SELECT *
  FROM "ANALYTICS"."ANALYTICS_SENSITIVE"."GREENHOUSE_RECRUITING_XF"
  WHERE offer_status IS NOT NULL
 
), candidate_names AS (
       
SELECT *
  FROM "RAW"."GREENHOUSE"."CANDIDATES"
   
),hires AS ( 
  
SELECT * 
  FROM "ANALYTICS"."ANALYTICS_SENSITIVE"."GREENHOUSE_HIRES"
     
), location_factor AS (

SELECT *
   FROM "ANALYTICS"."ANALYTICS"."EMPLOYEE_DIRECTORY_ANALYSIS"

)                
                        
SELECT 
    offers.offer_status AS offer_status,
    offers.application_status AS application_status,
    hire_date_mod AS hire_date,
    CONCAT(first_name,' ', last_name) AS candidate_name,
    offers.division_modified AS division,
    offers.department_name AS department,
    offers.job_name AS vacancy,
    offers.time_to_offer,
    offers.source_name AS source,
    application_date,
    offer_sent_date AS offer_sent,
    offer_resolved_date AS offer_accept,
    hires.region AS location,
    location_factor.location_factor AS location_factor,
    offers.candidate_id
  FROM offers
  LEFT JOIN candidate_names
    ON candidate_names.id = offers.candidate_id
  LEFT JOIN hires
    ON hires.candidate_id = offers.candidate_id
      AND hires.application_id = offers.application_id
  LEFT JOIN location_factor
    ON location_factor.date_actual = hires.hire_date_mod
    AND hires.employee_id = location_factor.employee_id
    WHERE DATE_TRUNC(month,offer_sent) = DATE_TRUNC(month,DATEADD(month,-1,CURRENT_DATE()))
