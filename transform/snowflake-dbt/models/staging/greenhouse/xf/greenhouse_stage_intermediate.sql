    {# {{ config({
        "schema": "analytics"
        })
    }} #}


{% set repeated_column_names = 
        "requisition_id,
        current_stage_name,
        application_status,
        job_name,
        department_name,
        division_modified,
        source_name,
        source_type,
        sourcer_name,
        candidate_recruiter,
        candidate_coordinator,
        rejection_reason_name,
        rejection_reason_type,
        current_job_req_status,
        is_hired_in_bamboo,
        time_to_offer" %}


WITH stages AS (

    SELECT *
    FROM {{ ref ('greenhouse_application_stages_source') }}
    WHERE stage_entered_on IS NOT NULL
    
), recruiting_xf AS (

    SELECT * 
    FROM {{ ref ('greenhouse_recruiting_xf') }}


), applications AS (

        SELECT 
        application_id,
        candidate_id,
        'Application Submitted'                                                         AS application_stage,
        TRUE                                                                            AS is_milestone_stage,
        DATE_TRUNC(MONTH, application_date)                                             AS application_month,
        application_date                                                                AS stage_entered_on,
        null                                                                            AS stage_exited_on,
        {{repeated_column_names}}
        FROM recruiting_xf 

    ), stages_intermediate AS (
    
        SELECT 
          stages.application_id,
          candidate_id,
          stages.stages_cleaned                                                           AS application_stage,
          stages.is_milestone_stage,
          DATE_TRUNC(MONTH, application_date)                                             AS application_month,
          IFF(application_stage_name = 'Offer',offer_sent_date, stages.stage_entered_on)  AS stage_entered_on,
          IFF(application_stage_name = 'Offer', offer_resolved_date, 
              COALESCE(stages.stage_exited_on, CURRENT_DATE()))                           AS stage_exited_on,
          {{repeated_column_names}}
        FROM stages
        LEFT JOIN recruiting_xf 
          ON recruiting_xf.application_id = stages.application_id
    
    ), hired_rejected AS (

        SELECT 
          application_id,
          candidate_id,
          IFF(application_status = 'hired','Hired', 'Rejected')                           AS application_stage,
          TRUE                                                                            AS is_milestone_stage,
          DATE_TRUNC(MONTH, application_date)                                             AS application_month,
          IFF(application_status = 'hired',candidate_target_hire_date, rejected_at)       AS stage_entered_on,
          IFF(application_status = 'hired',candidate_target_hire_date, rejected_at)       AS stage_exited_on,
          {{repeated_column_names}}
        FROM recruiting_xf 
        WHERE application_status in ('hired', 'rejected')
    
    ), all_stages AS (

        SELECT * 
        FROM applications 

        UNION ALL
    
        SELECT * 
        FROM stages_intermediate
    
        UNION ALL

        SELECT *
        FROM hired_rejected

    ), stages_hit AS (

        SELECT 
        application_id,
        candidate_id,
        MIN(stage_entered_on)                                                       AS min_stage_entered_on,
        MAX(stage_exited_on)                                                        AS max_stage_exited_on,
        SUM(IFF(application_stage = 'Application Submitted',1,0))                   AS hit_application_submitted,
        SUM(IFF(application_stage = 'Application Review',1,0))                      AS hit_application_review,
        SUM(IFF(application_stage = 'Assessment',1,0))                              AS hit_assessment,
        SUM(IFF(application_stage = 'Screen',1,0))                                  AS hit_screening,
        SUM(IFF(application_stage = 'Team Interview - Face to Face',1,0))           AS hit_team_interview,
        SUM(IFF(application_stage = 'Reference Check',1,0))                         AS hit_reference_check,
        SUM(IFF(application_stage = 'Offer',1,0))                                   AS hit_offer,
        SUM(IFF(application_stage = 'Hired',1,0))                                   AS hit_hired,
        SUM(IFF(application_stage = 'Rejected',1,0))                                AS hit_rejected
        FROM all_stages
        GROUP BY 1,2
        
    ), intermediate AS (

        SELECT 
          all_stages.*,
           ROW_NUMBER() OVER (PARTITION BY application_id, candidate_id 
                              ORDER BY stage_entered_on DESC)                       AS row_number_stages_desc
        FROM all_stages        

    ), stage_order_revamped AS (

        SELECT
          intermediate.*,
          CASE WHEN application_stage in ('Hired','Rejected') AND (hit_rejected = 1 or hit_hired = 1 )       
                  THEN 1
                WHEN (hit_rejected = 1 or hit_hired = 1 )   
                  THEN   row_number_stages_desc+1
                ELSE row_number_stages_desc END             AS row_number_stages_desc_updated
        FROM intermediate 
        LEFT JOIN stages_hit
          ON intermediate.application_id = stages_hit.application_id
          AND intermediate.candidate_id = stages_hit.candidate_id
    
    ), final AS (   

        SELECT
          {{ dbt_utils.surrogate_key('stage_order_revamped.application_id', 'stage_order_revamped.candidate_id') }} AS unique_key,
          stage_order_revamped.application_id,
          stage_order_revamped.candidate_id,
          application_stage, 
          is_milestone_stage,
          stage_entered_on,
          stage_exited_on,
          DATE_TRUNC(MONTH,stage_entered_on)                                          AS month_stage_entered_on,
          DATE_TRUNC(MONTH,stage_exited_on)                                           AS month_stage_exited_on,
          DATEDIFF(DAY, stage_entered_on, COALESCE(stage_exited_on, CURRENT_DATE()))  AS days_in_stage,
          DATEDIFF(DAY, min_stage_entered_on, max_stage_exited_on)                    AS days_in_pipeline,
          row_number_stages_desc_updated                                              AS row_number_stages_desc,
          LEAD(application_stage) OVER 
                (PARTITION BY stage_order_revamped.application_id, stage_order_revamped.candidate_id 
                 ORDER BY row_number_stages_desc_updated DESC)                        AS next_stage,
          IFF(row_number_stages_desc_updated = 1, TRUE, FALSE)                        AS is_current_stage,

          application_month,
          {{repeated_column_names}},
          hit_application_review,
          hit_assessment,
          hit_screening,
          hit_team_interview,
          hit_reference_check,
          hit_offer,
          hit_hired,
          hit_rejected
        FROM stage_order_revamped
        LEFT JOIN stages_hit 
          ON stage_order_revamped.application_id = stages_hit.application_id
          AND stage_order_revamped.candidate_id = stages_hit.candidate_id
        
)
            
SELECT *      
FROM final


