
{% set repeated_column_names = "month_date, gender" %}


WITH date_details AS (
  
    SELECT 
      date_actual                                                          AS month_date,                               
      'join'                                                               AS join_field  
    FROM {{ ref ('date_details') }}
    WHERE date_actual <= {{max_date_in_bamboo_analyses()}}
    AND day_of_month = 1 
    AND date_actual >= '2018-09-01'

), applications AS (
    
    SELECT * 
    FROM {{ ref ('greenhouse_applications') }}
    WHERE applied_at >= '2018-09-01'

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
      DISTINCT eeoc_field_name        AS eeoc_field_name,
      'join'                          AS join_field
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
      eeoc.eeoc_values,
      offers.offer_id,
      iff(offers.offer_status = 'accepted',1,0)                            AS accepted_offer
    FROM applications
    LEFT JOIN base
      ON date_trunc('month',applications.applied_at) = base.month_date
    LEFT JOIN eeoc            
      ON applications.application_id = eeoc.application_id
      AND eeoc.eeoc_field_name = base.eeoc_field_name
    LEFT JOIN offers
      ON applications.application_id = offers.application_id 
    WHERE base.month_date IS NOT NULL

), offers_aggregated AS (
  
    SELECT 
      DATE_TRUNC('month', offers.sent_at)                                   AS month_date, 
      COALESCE(candidate_gender,'Decline To Self Identify')                 AS gender,
      COUNT(DISTINCT(offer_id))                                             AS number_of_offers, 
      SUM(IFF(offer_status = 'accepted',1,0))                               AS accepted_offers,
      accepted_offers/number_of_offers                                      AS offer_acceptance_rate,
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
      {{repeated_column_names}},
      ratio_to_report(COUNT(candidate_id))
        OVER (PARTITION BY month_date)                                      AS metric_total,
      'percent_of_applicants'                                               AS recruiting_metric
    FROM candidates_aggregated
    GROUP BY 1,2
  
    UNION ALL
  
    SELECT 
      {{repeated_column_names}},
      SUM(hired)/SUM(total_applications)                                    AS metric_total,             
      'applicaton_to_offer_percent'                                         AS recruiting_metric 
    FROM candidates_aggregated
    GROUP BY 1,2  

    UNION ALL
  
    SELECT 
      {{repeated_column_names}},
      RATIO_TO_REPORT(number_of_offers) 
        OVER (PARTITION BY month_date)                                      AS metric_total,
      'percent_of_offers'                                                   AS recruiting_metric
    FROM offers_aggregated

    UNION ALL

    SELECT 
      {{repeated_column_names}},
      offer_acceptance_rate                                                 AS metric_total,  
      'offer_acceptance_rate_based_on_offer_month'                          AS recruiting_metric
    FROM offers_aggregated

    UNION ALL

    SELECT 
      {{repeated_column_names}},
      avg_apply_to_accept_days                                              AS metric_total,
      'avg_apply_to_accept_days'                                            AS recruiting_metric
    FROM offers_aggregated

), final AS(

    SELECT 
      date_details.month_date,
      gender,
      metric_total,
      recruiting_metric
    FROM date_details
    LEFT JOIN aggregated
    ON date_details.month_date = aggregated.month_date
    WHERE date_details.month_date < DATE_TRUNC('month', CURRENT_DATE)

)

SELECT *
FROM final 