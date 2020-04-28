WITH dates AS (

  SELECT date_actual
  FROM  {{ref('date_details')}}
  WHERE day_of_month = 1
    AND date_actual BETWEEN DATE_TRUNC(month, DATEADD(month,-13,CURRENT_DATE())) 
    AND DATE_TRUNC(month, DATEADD(month,-1,CURRENT_DATE()))
  
), location_factor AS (

  SELECT *
  FROM {{ref('greenhouse_location_factors')}}

), screened AS (

  SELECT *
  FROM  {{ref('greenhouse_stage_intermediate')}}
  WHERE lower(application_stage) like '%screen%'
  AND month_stage_entered_on >= '2019-06-01' -- started capturing location factor'
  AND month_stage_entered_on BETWEEN DATE_TRUNC(month,DATEADD(month,-13,CURRENT_DATE())) 
      AND DATE_TRUNC(month,DATEADD(month,-1,CURRENT_DATE()))   
  
), intermediate AS (

    SELECT 
      screened.month_stage_entered_on   AS application_screened_month, 
      screened.division_modified        AS division, 
      screened.department_name          AS department,
      screened.application_id, 
      COUNT(screened.application_id)    AS total_screened,
      AVG(location_factor)              AS location_factor
    FROM screened 
    LEFT JOIN location_factor
      ON screened.application_id = location_factor.application_id
    GROUP BY 1,2,3
  
), division_breakout AS (

    SELECT
        application_screened_month,
        'division_breakout'                 AS breakout_type,
        null                                AS department,
        division,
        COUNT(application_id)               AS total_applicants_screened,
        ROUND(AVG(location_factor),1)       AS average_location_factor,
        SUM(IFF(location_factor IS NOT NULL,1,0))/
            COUNT(application_id)           AS percent_of_applicants_with_location
    FROM intermediate
    GROUP BY 1,2,3

), department_breakout AS (
  
    SELECT
        application_screened_month,
        'department_breakout'               AS breakout_type,
        division,
        department,
        COUNT(application_id)               AS total_applicants_screened,
        ROUND(AVG(location_factor),1)       AS average_location_factor,
        SUM(IFF(location_factor IS NOT NULL,1,0))/
            COUNT(application_id)           AS percent_of_applicants_with_location
    FROM intermediate
    GROUP BY 1,2,3,4
  
), unioned_data AS (

    SELECT * 
    FROM division_breakout

    UNION ALL

    SELECT * 
    FROM department_breakout
  
)

SELECT 
  DATE_TRUNC(month, dates.date_actual) AS month_screened,
  breakout_type,
  division,
  department,
  total_applicants_screened,
  average_location_factor/100          AS average_location_factor,
  percent_of_applicants_with_location
FROM dates
LEFT JOIN unioned_data
  ON dates.date_actual = unioned_data.application_screened_month 
WHERE total_applicants_screened>=5


