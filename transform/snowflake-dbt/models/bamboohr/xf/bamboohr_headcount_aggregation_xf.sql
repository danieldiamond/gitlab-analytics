With dates AS (
      
    SELECT 
      date_actual                                 AS start_date,
      LAST_DAY(date_actual)                       AS end_date
    FROM "ANALYTICS"."ANALYTICS"."DATE_DETAILS"
    WHERE date_day <= LAST_DAY(current_date) 
       AND day_of_month = 1 
    
), mapping AS (
  
  SELECT * 
  FROM "ANALYTICS"."ANALYTICS_SENSITIVE"."BAMBOOHR_ID_EMPLOYEE_NUMBER_MAPPING"
    
), employees AS (  
  
    SELECT 
      employees.*,
      coalesce(mapping.gender,'unidentified')             AS gender,
      coalesce(mapping.ethnicity, 'unidentified')         AS ethnicity,
      coalesce(mapping.nationality, 'unidentified')       AS nationality,
      coalesce(mapping.region, 'unidentified')            AS region,
      ROW_NUMBER() OVER (PARTITION BY employees.employee_id ORDER BY valid_from_date) AS employee_status_event
    FROM "ANALYTICS"."ANALYTICS_SENSITIVE"."BAMBOOHR_EMPLOYMENT_STATUS_XF"  employees
    LEFT JOIN mapping 
       ON employees.employee_id = mapping.employee_id
      
),headcount_start AS (
 
  SELECT 
    start_date                                          AS month_date,            
    gender,
    ethnicity,
    nationality,
    region,
    COUNT(employee_id)                                  AS headcount_start
  FROM dates
  LEFT JOIN employees
     ON dates.start_date BETWEEN valid_from_date AND valid_to_date
  GROUP BY 1,2,3,4,5      
  
), headcount_end AS (
  
    SELECT 
      end_date,
      gender,
      ethnicity,
      nationality,
      region,
      COUNT(employee_id)                                  AS headcount_end  
  FROM dates
  LEFT JOIN employees 
    ON dates.end_date BETWEEN valid_from_date AND valid_to_date
  GROUP BY 1,2,3,4,5  
  
), separated AS (
  
    SELECT 
      DATE_TRUNC('month',dates.start_date)                AS separation_month,
      gender,
      ethnicity,
      nationality,
      region,
      COUNT(employee_id)                                  AS total_separated,
      SUM(IFF(termination_type = 'Voluntary',1,0))        AS voluntary_separation,
      SUM(IFF(termination_type = 'Involuntary',1,0))      AS involuntary_separation  
  FROM dates
  LEFT JOIN employees 
    ON DATE_TRUNC('month',dates.start_date) = DATE_TRUNC('month',valid_from_date) 
  WHERE employment_status='Terminated'
  GROUP BY 1,2,3,4,5
  
), hires AS (

    SELECT 
      date_trunc('month', dates.start_date)          AS hire_month,
      gender,
      ethnicity,
      nationality,
      region,
      SUM(IFF(employee_status_event = 1,1,0))        AS total_hired
    FROM dates 
    LEFT JOIN employees 
       ON DATE_TRUNC('month',dates.start_date) = DATE_TRUNC('month',valid_from_date) 
    WHERE employment_status<>'Terminated'
    GROUP BY 1,2,3,4,5    
  
), aggregated AS (

    SELECT
      month_date,
      gender,
      ethnicity,
      nationality,
      region,
      headcount_start               AS total_count,
     'headcount_start'              AS metric
    FROM headcount_start                                
    
    UNION ALL
  
    SELECT
       headcount_end.*,
       'headcount_end'              AS Metric
    FROM headcount_end
 
    UNION ALL
  
    SELECT 
       hires.*,
       'hires'                      AS Metric
    FROM hires
  
    UNION ALL

    SELECT  
      separation_month, 
      gender,
      ethnicity,
      nationality,
      region,
      total_separated,
      'total_separated'             AS Metric
    FROM separated
  
    UNION ALL
  
    SELECT
      separation_month, 
      gender,
      ethnicity,
      nationality,
      region,
      voluntary_separation,
      'voluntary_separations'       AS Metric
    FROM separated
  
    UNION ALL
  
    SELECT
      separation_month, 
      gender,
      ethnicity,
      nationality,
      region,
      involuntary_separation,
      'involuntary_separations'     AS Metric
  FROM separated
  
 )
 
 SELECT * 
 FROM aggregated
 
