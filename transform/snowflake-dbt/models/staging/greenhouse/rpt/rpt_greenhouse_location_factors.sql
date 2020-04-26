WITH location_factor AS (

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
      screened.application_id, 
      COUNT(screened.application_id)    AS total_screened,
      AVG(location_factor)              AS location_factor
    FROM screened 
    LEFT JOIN location_factor
      ON screened.application_id = location_factor.application_id
    GROUP BY 1,2,3
  
), final AS (

    SELECT
        application_screened_month,
        division,
        COUNT(application_id)               AS total_applicants_screened,
        ROUND(AVG(location_factor),1)       AS average_location_factor,
        SUM(IFF(location_factor IS NOT NULL,1,0))/
            COUNT(application_id)           AS percent_of_applicants_with_location
    FROM intermediate
    GROUP BY 1,2

)

SELECT * 
FROM final

