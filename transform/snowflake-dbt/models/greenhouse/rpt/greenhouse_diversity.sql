{% set repeated_column_metrics = 
    "COUNT(CASE WHEN capture_month = 'application_month' THEN application_id ELSE NULL END)         AS total_candidates_applied,
    SUM(CASE WHEN capture_month = 'application_month' THEN accepted_offer ELSE NULL END)            AS total_offers_based_on_application_month,
    SUM(CASE WHEN capture_month='offer_sent_month' THEN 1 ELSE NULL END)                            AS total_sent_offers,
    SUM(CASE WHEN capture_month='offer_sent_month' THEN accepted_offer ELSE NULL END)               AS offers_accepted_based_on_sent_month,
    SUM(CASE WHEN capture_month = 'accepted_month' THEN accepted_offer ELSE NULL END)               AS offers_accepted,
    AVG(CASE WHEN capture_month = 'accepted_month' THEN apply_to_accept_days ELSE NULL END)         AS time_to_offer   
    " %}

{% set repeated_column_names_ratio_to_report =
                "(partition by month_date, breakout_type, department_name, division, eeoc_field_name order by month_date)" %}

WITH greenhouse_diversity_intermediate AS (

    SELECT * 
    FROM  {{ ref ('greenhouse_diversity_intermediate') }}

), breakout AS (

    SELECT
      month_date,
      'kpi_level'                                 AS breakout_type,
      'NA'                                        AS department_name,
      'NA'                                        AS division,
      'NA'                                        AS eeoc_field_name,
      'NA'                                        AS eeoc_values,
      {{repeated_column_metrics}}  
    FROM  greenhouse_diversity_intermediate
    WHERE eeoc_field_name = 'candidate_status'
    GROUP BY 1,2,3,4,5,6

    UNION ALL

    SELECT
      month_date,
      'all'                                 AS breakout_type,
      department_name,
      division,
      eeoc_field_name,
      eeoc_values,
      {{repeated_column_metrics}}
    FROM  greenhouse_diversity_intermediate
    GROUP BY 1,2,3,4,5,6

    UNION ALL

    SELECT
      month_date,
      'division_breakout'                     AS breakout_type,
      'division_breakout'                     AS department_name,
      division,
      eeoc_field_name,
      eeoc_values,
      {{repeated_column_metrics}}
    FROM  greenhouse_diversity_intermediate
    GROUP BY 1,2,3,4,5,6

    UNION ALL

    SELECT
      month_date,
      'eeoc_only_breakout'                    AS breakout_type,
      'NA'                                    AS department_name,
      'NA'                                    AS division,
      eeoc_field_name,
      eeoc_values,
      {{repeated_column_metrics}}
    FROM  greenhouse_diversity_intermediate
    group by 1,2,3,4,5,6

), aggregated AS (

    SELECT 
      breakout.*,
      CASE WHEN total_sent_offers = 0 THEN NULL
           WHEN total_sent_offers IS NULL THEN NULL
           ELSE offers_accepted_based_on_sent_month/total_sent_offers END AS offer_acceptance_rate_based_on_offer_month,
      IFF(total_candidates_applied = 0 , null,
          RATIO_TO_REPORT(total_candidates_applied) 
          OVER {{repeated_column_names_ratio_to_report}})            AS percent_of_applicants,
      IFF(total_sent_offers = 0, null,      
          RATIO_TO_REPORT(total_sent_offers) 
          OVER {{repeated_column_names_ratio_to_report}})            AS percent_of_offers_sent,
      IFF(offers_accepted=0, null,
          RATIO_TO_REPORT(offers_accepted) 
          OVER {{repeated_column_names_ratio_to_report}})             AS percent_of_offers_accepted 
    FROM breakout
    
), final AS (

    SELECT 
    month_date,
    breakout_type,
    department_name,
    division,
    eeoc_field_name,
    eeoc_values,
    total_candidates_applied,
    percent_of_applicants,
    total_offers_based_on_application_month/total_candidates_applied      AS application_to_offer_percent,
    percent_of_offers_sent,
    percent_of_offers_accepted,
    time_to_offer,
    offer_acceptance_rate_based_on_offer_month
    FROM aggregated
    WHERE total_candidates_applied > 5

)

SELECT *
FROM final 

