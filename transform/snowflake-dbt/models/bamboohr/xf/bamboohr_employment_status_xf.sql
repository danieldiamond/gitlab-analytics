{{ config({
    "schema": "analytics"
    })
}}

WITH bamboohr_employment_status AS (
  
    SELECT *
    FROM {{ ref ('bamboohr_employment_status') }}
  
),employment_log AS (--the emp_status will need to work with people group to create a translation table for different employment statuses   
  
   SELECT
    status_id,
    employee_id,
    employment_status,
    termination_type,
    effective_date                                                                      AS valid_from_date,
    LEAD(effective_date) OVER (PARTITION BY employee_id ORDER BY effective_date)        AS valid_to_date,
    LEAD(employment_status) OVER (PARTITION BY employee_id ORDER BY effective_date)     AS next_employment_status,
    LAG(employment_status) OVER (PARTITION BY employee_id ORDER BY effective_date)      AS previous_employment_status,
    CASE 
      WHEN employment_status LIKE '%Leave%' 
        THEN 'L'
      WHEN employment_status IN ('Terminated','Suspended') 
        THEN 'T'
      WHEN employment_status ='Active' 
        THEN 'A'
      ELSE'Other' END                                                                   AS emp_status                   
    FROM bamboohr_employment_status

), final AS (  

    SELECT
      employee_id,
      employment_status,
      emp_status, 
      termination_type,
            CASE WHEN previous_employment_status ='Terminated' 
              AND employment_status !='Terminated' THEN 1 ELSE 0 END                    AS is_rehire,
      next_employment_status,           
      to_date(valid_from_date)                                                          AS valid_from_date,
      IFF(employment_status='Terminated'
            ,to_date(valid_to_date)
            ,IFNULL(to_date(valid_to_date), last_day(current_date)))                    AS valid_to_date
     FROM employment_log
)

SELECT *
FROM final



