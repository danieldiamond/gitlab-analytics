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
    lead(effective_date) OVER (PARTITION BY employee_id ORDER BY effective_date)        AS valid_to_date,
    lead(employment_status) OVER (PARTITION BY employee_id ORDER BY effective_date)     AS next_employment_status,
    lag(employment_status) OVER (PARTITION BY employee_id ORDER BY effective_date)      AS previous_employment_status,
    case 
      when employment_status like '%Leave%' 
        then 'L'
      when employment_status in ('Terminated','Suspended') 
        then 'T'
      when employment_status ='Active' 
        then 'A'
      else 'Other' end                                                                AS emp_status                   
    FROM bamboohr_employment_status

), final AS (  

    SELECT
      employee_id,
      employment_status,
      emp_status, -- will need to work with people group to create a translation table for different employment statuses
      termination_type,
            case when previous_employment_status ='Terminated' 
              and employment_status !='Terminated' then 1 else 0 end                               AS is_rehire,
      next_employment_status,           
      to_date(valid_from_date)                                                               AS valid_from_date,
      ifnull(to_date(valid_to_date), last_day(current_date))                                 AS valid_to_date
     FROM employment_log
)

SELECT *
FROM final



