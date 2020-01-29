{{ config({
    "materialized":"table",
    "schema": "analytics"
    })
}}

With date_details AS (
  
    SELECT 
      date_actual                                       AS start_date,
      last_day(date_actual)                             AS end_date
    FROM {{ ref ('date_details') }}
    WHERE date_day <= LAST_DAY(current_date) 
    AND day_of_month = 1 

), applications AS (
    
    SELECT * 
 	FROM analytics.analytics.greenhouse_applications

), offers AS (
    
    SELECT *
    FROM {{ ref ('greenhouse_offers') }}
  
), eeoc AS (

    SELECT *
    FROM {{ ref ('greenhouse_eeoc_responses') }}
  
), candidates_aggregated as ( 
  
    SELECT 
      candidate_id,
      offers.offer_id,
      DATE_TRUNC('month', MIN(applied_at))                                  AS application_month, 
      COALESCE(candidate_gender,'Decline To Self Identify')                AS gender,
      SUM(iff(offers.offer_id IS NOT NULL,1,0))                             AS hired,
      COUNT(DISTINCT(applications.application_id))                          AS total_applications
    FROM applications
    LEFT JOIN eeoc            
      ON applications.application_id = eeoc.application_id
    LEFT JOIN offers
      ON applications.application_id = offers.application_id
    GROUP BY 1,2,4
  
), offers_aggregated AS (
  
    SELECT 
      DATE_TRUNC('month', offers.sent_at)                                   AS offer_sent_month, 
      COALESCE(candidate_gender,'Decline To Self Identify')                 AS gender,
      COUNT(DISTINCT(offer_id))                                             AS number_of_offers, 
      SUM(IFF(offer_status = 'accepted',1,0))                               AS accepted_offers,
      SUM(IFF(offer_status = 'accepted',1,0))
            /COUNT(distinct(offer_id))                                      AS offer_acceptance_rate,
      AVG(IFF(offer_status ='accepted',
            DATEDIFF('day', applications.applied_at, offers.sent_at),
            NULL))                                                          AS avg_apply_to_accept_days
    FROM offers
    LEFT JOIN applications    
      ON offers.application_id = applications.application_id
    LEFT JOIN eeoc            
      ON offers.application_id = eeoc.application_id
    WHERE sent_at IS NOT NULL
    GROUP BY 1,2    
  
), aggregated AS (

    SELECT
      application_month                                                     AS metric_month,
      gender,
      ratio_to_report(COUNT(candidate_id))
        OVER (PARTITION BY application_month)                               AS metric_total,
      'percent_of_applicants'                                               AS recruiting_metric
    FROM candidates_aggregated
    GROUP BY 1,2
  
    UNION ALL
  
    SELECT 
      application_month,
      gender,
      SUM(hired)/SUM(total_applications)                                    AS metric_total,             
      'applicaton_to_offer_percent'                                         AS recruiting_metric 
    FROM candidates_aggregated
    GROUP BY 1,2  

    UNION ALL
  
    SELECT 
      offer_sent_month,
      gender,
      RATIO_TO_REPORT(number_of_offers) 
        OVER (PARTITION BY offer_sent_month)                                AS metric_total,
      'percent_of_offers'                                                   AS recruiting_metric
    FROM offers_aggregated

    UNION ALL

    SELECT 
      offer_sent_month,
      gender,
      offer_acceptance_rate                                                 AS metric_total,  
      'offer_acceptance_rate_based_on_offer_month'                          AS recruiting_metric
    FROM offers_aggregated

    UNION ALL

    SELECT 
      offer_sent_month,
      gender,
      avg_apply_to_accept_days                                              AS metric_total,
      'avg_apply_to_accept_days'                                            AS recruiting_metric
    FROM offers_aggregated

)

SELECT *  
FROM aggregated
