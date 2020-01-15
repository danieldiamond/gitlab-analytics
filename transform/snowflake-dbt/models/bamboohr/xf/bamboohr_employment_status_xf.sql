WITH bamboohr_employment_status AS (
  
    SELECT *
    FROM {{ ref ('bamboohr_employment_status') }}
  
), employment_log AS (
  
   SELECT
    status_id,
    employee_id,
    employment_status,
    termination_type,
    effective_date                                                                          AS valid_from_date,
    LEAD(effective_date) OVER (PARTITION BY employee_id ORDER BY effective_date)            AS valid_to_date,
    LEAD(employment_status) OVER (PARTITION BY employee_id ORDER BY effective_date)         AS next_employment_status,
    LAG(employment_status) OVER (PARTITION BY employee_id ORDER BY effective_date)          AS previous_employment_status
    FROM bamboohr_employment_status

), final AS (  

    SELECT
      employee_id,
      employment_status, 
      termination_type,
      CASE WHEN previous_employment_status ='Terminated' 
        AND employment_status !='Terminated' THEN 'True' ELSE 'False' END                   AS is_rehire,
      next_employment_status,           
      valid_from_date                                                                       AS valid_from_date,
      IFF(employment_status='Terminated'
            ,valid_to_date
            ,COALESCE(valid_to_date, date_trunc('week', dateadd(week, 3, CURRENT_DATE))))    AS valid_to_date
     FROM employment_log
)

SELECT *
FROM final



