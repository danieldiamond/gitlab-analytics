{% set repeated_column_metrics = 
    "COUNT(CASE WHEN capture_month = 'application_month' THEN application_id ELSE NULL END)         AS total_candidates_applied,
    SUM(CASE WHEN capture_month = 'application_month' THEN accepted_offer ELSE NULL END)            AS total_offers_based_on_application_month,
    SUM(CASE WHEN capture_month='offer_sent_month' THEN 1 ELSE NULL END)                            AS total_sent_offers,
    SUM(CASE WHEN capture_month='offer_sent_month' THEN accepted_offer ELSE NULL END)               AS offers_accepted_based_on_sent_month,
    SUM(CASE WHEN capture_month = 'accepted_month' THEN accepted_offer ELSE NULL END)               AS offers_accepted,
    AVG(CASE WHEN capture_month = 'accepted_month' THEN time_to_offer ELSE NULL END)                AS time_to_offer_average, 
    MEDIAN(CASE WHEN capture_month = 'accepted_month' THEN time_to_offer ELSE NULL END)             AS time_to_offer_median
    " %}

{% set repeated_column_names_ratio_to_report =
                "(partition by month_date, breakout_type, department_name, division, eeoc_field_name order by month_date) " %}

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
    WHERE lower(eeoc_field_name) = 'candidate_status'
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
      CASE WHEN total_candidates_applied = 0 THEN NULL
           WHEN total_candidates_applied IS NULL THEN NULL
           ELSE total_offers_based_on_application_month/total_candidates_applied END  AS application_to_offer_percent,
      CASE WHEN total_sent_offers = 0 THEN NULL
           WHEN total_sent_offers IS NULL THEN NULL
           ELSE offers_accepted_based_on_sent_month/total_sent_offers END             AS offer_acceptance_rate_based_on_offer_month,
      IFF(total_candidates_applied = 0 , null,
          RATIO_TO_REPORT(total_candidates_applied) 
          OVER {{repeated_column_names_ratio_to_report}})                             AS percent_of_applicants,
      IFF(total_sent_offers = 0, null,      
          RATIO_TO_REPORT(total_sent_offers) 
          OVER {{repeated_column_names_ratio_to_report}})                             AS percent_of_offers_sent,
      IFF(offers_accepted=0, null,
          RATIO_TO_REPORT(offers_accepted) 
          OVER {{repeated_column_names_ratio_to_report}})                             AS percent_of_offers_accepted,
      MIN(total_candidates_applied) OVER {{repeated_column_names_ratio_to_report}}    AS min_applicants_breakout, 
      MIN(total_sent_offers) OVER {{repeated_column_names_ratio_to_report}}           AS min_sent_offers_for_breakout,
      MIN(offers_accepted) OVER {{repeated_column_names_ratio_to_report}}             AS min_total_offers_accepted_for_breakout
    FROM breakout
    
), final AS (

    SELECT 
    month_date,
    breakout_type,
    department_name,
    division,
    LOWER(eeoc_field_name)                                                            AS eeoc_field_name,
    eeoc_values,
    ----Applicant Level----

    iff(min_applicants_breakout < 3, null, total_candidates_applied)                  AS total_candidates_applied,
    iff(breakout_type in ('kpi_level','eeoc_only_breakout'), 
                application_to_offer_percent, null)                                   AS application_to_offer_percent,
    iff(min_applicants_breakout < 3, null, percent_of_applicants)                     AS percent_of_applicants,

    ---Offers Sent Level ---

    iff(min_sent_offers_for_breakout < 3, null, total_sent_offers)                      AS total_sent_offers,
    iff(min_sent_offers_for_breakout < 3, null, percent_of_offers_sent)                 AS percent_of_offers_sent,
    min_sent_offers_for_breakout,

    --Offers Accepted Level ---
    iff(min_total_offers_accepted_for_breakout < 3, null, offers_accepted)              AS offers_accepted,
    iff(min_total_offers_accepted_for_breakout < 3, null, percent_of_offers_accepted)   AS percent_of_offers_accepted,
    iff(min_total_offers_accepted_for_breakout < 3, null, time_to_offer_average)        AS time_to_offer_average,
     iff(min_total_offers_accepted_for_breakout < 3, null, time_to_offer_median)        AS time_to_offer_median,
    iff(min_total_offers_accepted_for_breakout < 3, null, 
        offer_acceptance_rate_based_on_offer_month)                                     AS offer_acceptance_rate_based_on_offer_month 
    FROM aggregated
    WHERE month_date <= DATEADD(MONTH,-1,current_date())

)

SELECT * 
FROM final
