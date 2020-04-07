    {# {{ config({
        "schema": "analytics"
        })
    }} #}


{% set repeated_column_names = 
        "requisition_id,
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
        is_hired_in_bamboo,
        time_to_offer" %}


WITH stages AS (

    SELECT *
    FROM {{ ref ('greenhouse_application_stages') }}
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
        1                                                                               AS row_number_stages_asc,
        null                                                                            AS row_number_stages_desc,
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
        ROW_NUMBER() OVER (PARTITION BY stages.application_id, candidate_id 
                            ORDER BY stage_entered_on)                                  AS row_number_stages_asc,
        ROW_NUMBER() OVER (PARTITION BY stages.application_id, candidate_id 
                            ORDER BY stage_entered_on DESC)                             AS row_number_stages_desc,
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
        null                                                                            AS row_number_stages_asc,
        1                                                                               AS row_number_stages_desc,
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
        MIN(stage_entered_on)                                       AS min_stage_entered_on,
        MAX(stage_exited_on)                                        AS max_stage_exited_on,
        SUM(IFF(application_stage = 'Application Submitted',1,0))   AS hit_application_submitted,
        SUM(IFF(application_stage = 'Application Review',1,0))  AS hit_application_review,
        SUM(IFF(application_stage = 'Assessment',1,0))          AS hit_assessment,
        SUM(IFF(application_stage = 'Screen',1,0))              AS hit_screening,
        SUM(IFF(application_stage = 'Team Interview',1,0))      AS hit_team_interview,
        SUM(IFF(application_stage = 'Reference Check',1,0))     AS hit_reference_check,
        SUM(IFF(application_stage = 'Offer',1,0))               AS hit_offer,
        SUM(IFF(application_stage = 'hired',1,0))               AS hit_hired,
        SUM(IFF(application_stage = 'rejected',1,0))            AS hit_rejected
        FROM all_stages
        GROUP BY 1,2
        
    ), final AS (

        SELECT 
        {{ dbt_utils.surrogate_key('all_stages.application_id', 'all_stages.candidate_id') }} AS unique_key,
        all_stages.application_id,
        all_stages.candidate_id,
        application_stage, 
        is_milestone_stage,
        application_month,
        stage_entered_on,
        stage_exited_on,
        DATE_TRUNC(MONTH,stage_entered_on)                                          AS month_stage_entered_on,
        DATE_TRUNC(MONTH,stage_exited_on)                                           AS month_stage_exited_on,
        DATEDIFF(DAY, stage_entered_on, COALESCE(stage_exited_on, CURRENT_DATE()))  AS days_in_stage,
        DATEDIFF(DAY, min_stage_entered_on, max_stage_exited_on)                    AS days_in_pipeline,
        {{repeated_column_names}},
        hit_application_review,
        hit_assessment,
        hit_screening,
        hit_team_interview,
        hit_reference_check,
        hit_offer,
        hit_hired,
        hit_rejected,
        IFF(application_stage IN ('Hired','Rejected'), 1,
            ROW_NUMBER() OVER (PARTITION BY all_stages.application_id, all_stages.candidate_id 
                ORDER BY stage_entered_on DESC))                              AS row_number_stages_desc
        FROM all_stages
        LEFT JOIN stages_hit 
          ON all_stages.application_id = stages_hit.application_id
          AND all_stages.candidate_id = stages_hit.candidate_id
        
    )

    SELECT *,
      ROW_NUMBER() OVER (PARTITION BY application_id, candidate_id ORDER BY row_number_stages_desc DESC) AS row_number_stages_asc,
      LEAD(application_stage) OVER (PARTITION BY application_id, candidate_id ORDER BY row_number_stages_asc) AS next_stage,
      IFF(row_number_stages_desc = 1, TRUE, FALSE) AS current_stage
    FROM final