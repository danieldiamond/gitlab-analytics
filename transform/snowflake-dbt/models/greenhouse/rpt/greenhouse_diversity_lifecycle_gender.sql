{% set repeated_column_names = "month_date, eeoc_field_name, eeoc_values " %}

WITH date_details AS (
  
    SELECT 
      date_actual                                                          AS month_date,                               
      'join'                                                               AS join_field  
    FROM {{ ref ('date_details') }}
    WHERE date_actual <= {{max_date_in_bamboo_analyses()}}
    AND day_of_month = 1 
    AND date_actual >= '2018-09-01' -- 1st date we started capturing eeoc data

), applications AS (
    
    SELECT * 
    FROM {{ ref ('greenhouse_applications') }}

), offers AS (
    
    SELECT *
    FROM {{ ref ('greenhouse_offers') }}
  
), eeoc AS (

        {{ dbt_utils.unpivot(
        relation=ref('greenhouse_eeoc_responses'),
        cast_to='varchar',
        exclude=['application_id'],
        remove=['eeoc_response_submitted_at'],
        field_name='eeoc_field_name',
        value_name='eeoc_values'
        ) }}

), eeoc_fields AS (

    SELECT 
      DISTINCT eeoc_field_name                                  AS eeoc_field_name,
      'join'                                                    AS join_field
    FROM eeoc

), base AS (

    SELECT
      month_date,
      eeoc_field_name
    FROM date_details
    LEFT JOIN eeoc_fields 
      ON eeoc_fields.join_field = date_details.join_field

), candidates_aggregated AS (
  
    SELECT 
      base.*,
      applications.candidate_id,
      iff(eeoc_values in ('I don''t wish to answer','Decline To Self Identify'), 
            'Did Not Identify',
            coalesce(eeoc_values, 'Did Not Identify'))                                  AS eeoc_values,
      COUNT(DISTINCT(applications.application_id))                                      AS total_applications,
      sum(iff(offers.offer_status = 'accepted',1,0))                                    AS accepted_offer
    FROM applications
    LEFT JOIN base
      ON date_trunc('month',applications.applied_at) = base.month_date
    LEFT JOIN eeoc            
      ON applications.application_id = eeoc.application_id
      AND eeoc.eeoc_field_name = base.eeoc_field_name
    LEFT JOIN offers
      ON applications.application_id = offers.application_id 
    WHERE base.month_date IS NOT NULL
    group by 1,2,3,4

), offers_aggregated AS (
  
    SELECT 
      base.*,
      candidate_id,
      offer_id,
      iff(eeoc_values in ('I don''t wish to answer','Decline To Self Identify'), 
            'Did Not Identify',
            coalesce(eeoc_values, 'Did Not Identify'))                              AS eeoc_values,
      IFF(offer_status = 'accepted',1,0)                                            AS accepted_offer,
      IFF(offer_status ='accepted',
            DATEDIFF('day', applications.applied_at, offers.sent_at),
            NULL)                                                                   AS apply_to_accept_days
    FROM base
    LEFT JOIN offers 
      ON DATE_TRUNC('month', offers.sent_at) = base.month_date
    LEFT JOIN applications    
      ON offers.application_id = applications.application_id
    LEFT JOIN eeoc            
      ON offers.application_id = eeoc.application_id
      AND eeoc.eeoc_field_name = base.eeoc_field_name
    WHERE base.month_date IS NOT NULL

), aggregated AS (

    SELECT
      {{repeated_column_names}},
      RATIO_TO_REPORT(COUNT(DISTINCT(candidate_id)))
        OVER (PARTITION BY month_date, eeoc_field_name)                     AS metric_total,
      'percent_of_applicants'                                               AS recruiting_metric
    FROM candidates_aggregated
    GROUP BY 1,2,3
  
    UNION ALL
  
    SELECT 
      {{repeated_column_names}},
      SUM(accepted_offer)/COUNT(DISTINCT(candidate_id))                     AS metric_total,             
      'application_to_offer_percent'                                         AS recruiting_metric 
    FROM candidates_aggregated
    GROUP BY 1,2,3  

    UNION ALL
  
    SELECT 
      {{repeated_column_names}},
      RATIO_TO_REPORT(COUNT(DISTINCT(offer_id)))
        OVER (PARTITION BY month_date, eeoc_field_name)                     AS metric_total,
      'percent_of_extended_offers'                                          AS recruiting_metric
    FROM offers_aggregated
    GROUP BY 1,2,3

    UNION ALL
  
    SELECT 
      {{repeated_column_names}},
      RATIO_TO_REPORT(SUM(accepted_offer)) 
        OVER (PARTITION BY month_date, eeoc_field_name)                     AS metric_total,
      'percent_of_accepted_offers'                                          AS recruiting_metric
    FROM offers_aggregated
    GROUP BY 1,2,3

    UNION ALL

    SELECT 
      {{repeated_column_names}},
      SUM(accepted_offer)/ COUNT(DISTINCT(offer_id))                        AS metric_total,  
      'offer_acceptance_rate_based_on_offer_month'                          AS recruiting_metric
    FROM offers_aggregated
    GROUP BY 1,2,3

    UNION ALL

    SELECT 
      {{repeated_column_names}},
      AVG(apply_to_accept_days)                                             AS metric_total,
      'avg_apply_to_accept_days'                                            AS recruiting_metric
    FROM offers_aggregated
    GROUP BY 1,2,3 

)

SELECT *
FROM aggregated
WHERE month_date < DATE_TRUNC('month', CURRENT_DATE) 
