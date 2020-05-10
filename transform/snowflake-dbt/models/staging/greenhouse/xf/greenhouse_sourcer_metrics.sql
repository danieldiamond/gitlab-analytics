WITH date_details AS (

    SELECT 
      date_actual   AS month_date,                             
      1             AS join_field  
    FROM {{ ref ('date_details') }}
    WHERE date_actual BETWEEN DATE_TRUNC(month, DATEADD(month,-5,CURRENT_DATE())) AND CURRENT_DATE()
      AND day_of_month = 1 

), recruiting_data AS (
    
    SELECT *,
      1 AS join_field
    FROM {{ ref ('greenhouse_stage_analysis') }}
    WHERE sourcer_name IS NOT NULL
      AND source_type = 'Prospecting'

), base AS (

    SELECT 
      date_details.month_date,
      recruiting_data.sourcer_name
    FROM date_details
    LEFT JOIN recruiting_data
      ON date_details.join_field = recruiting_data.join_field
    GROUP BY 1,2

), metrics AS (

    SELECT 
      month_date,
      base.sourcer_name,
      SUM(IFF(application_stage = 'Application Submitted',1,0))                                                             AS prospected,
      IFF(prospected = 0, NULL, 
         (SUM(IFF(application_stage = 'Application Submitted' AND hit_application_review,1,0))/ prospected))                AS prospect_to_review,
      IFF(prospected = 0, NULL, (SUM(IFF(application_stage = 'Application Submitted' AND hit_screening,1,0))/prospected))   AS prospect_to_screen,
      IFF(prospected = 0, NULL, SUM(IFF(application_stage = 'Application Submitted' AND hit_hired,1,0))/prospected)         AS prospect_to_hire
      {# IFF(prospected = 0, NULL, SUM(IFF(application_stage = 'Application Submitted' 
                                          AND hit_team_interview = 0 
                                          AND hit_rejected = 1
                                          AND rejection_reason_type = 'They rejected us'))/prospected)                      AS prospect_to_dropout,

      SUM(IFF(application_stage = 'Application Review',1,0))                                                                AS app_reviewed,
      IFF(app_reviewed = 0, NULL, (SUM(IFF(application_stage = 'Application Review' AND hit_screening,1,0))/app_reviewed))  AS review_to_screen,
      IFF(app_reviewed = 0, NULL, SUM(IFF(application_stage = 'Application Review' AND hit_hired,1,0))/app_reviewed)        AS review_to_hire,
  

      SUM(IFF(application_stage = 'Screen',1,0))                                                                            AS screen,
      IFF(screen = 0, NULL, SUM(IFF(application_stage = 'Screen' AND hit_team_interview,1,0))/screen)                       AS screen_to_interview,
      IFF(screen = 0, NULL, SUM(IFF(application_stage = 'Screen' AND hit_hired,1,0))/screen)                                AS screen_to_hire,
  

      SUM(IFF(application_stage = 'Team Interview - Face to Face',1,0))                                                     AS team_interview,
      IFF(team_interview = 0, NULL, 
            SUM(IFF(application_stage = 'Team Interview - Face to Face' AND hit_hired,1,0))/team_interview)                 AS interview_to_hire,
      IFF(team_interview = 0, NULL, 
            SUM(IFF(application_stage = 'Team Interview - Face to Face' AND hit_rejected,1,0))/team_interview)              AS interview_to_reject,

      SUM(IFF(application_stage = 'Executive Interview',1,0))                                                               AS executive_interview,
      IFF(executive_interview = 0, NULL, 
            SUM(IFF(application_stage = 'Executive Interview' AND hit_hired,1,0))/executive_interview)                      AS exec_interview_to_hire,
    
      SUM(IFF(application_stage = 'Reference Check',1,0))                                                                   AS reference_check,

      SUM(IFF(application_stage = 'Rejected' 
                AND hit_team_interview=0 
                AND rejection_reason_type = 'They rejected us',1,0))                                                        AS candidate_drop_out,

      SUM(IFF(application_stage = 'Offer',1,0))                                                                             AS offer,
      IFF(offer=0,0, (SUM(IFF(application_stage = 'Application Submitted' AND hit_offer,1,0))/offer))                       AS app_to_offer_rate,

      SUM(IFF(application_stage = 'Hired',1,0))                                                                             AS hired #}
    FROM base
    LEFT JOIN recruiting_data
      ON base.month_date = recruiting_data.month_stage_entered_on
      AND base.sourcer_name = recruiting_data.sourcer_name
    GROUP BY 1,2
  
)

SELECT *
FROM metrics