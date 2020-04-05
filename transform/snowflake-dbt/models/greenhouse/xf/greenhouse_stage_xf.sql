    {{ config({
        "schema": "analytics"
        })
    }}

    WITH stages AS (

        SELECT *
        FROM {{ ref ('greenhouse_application_stages') }}
        WHERE stage_entered_on IS NOT NULL
    
    ), recruiting_xf AS (

    SELECT * 
    FROM {{ ref ('greenhouse_recruiting_xf') }}

    ), stages_intermediate AS (
    
        SELECT 
        stages.application_id,
        candidate_id,
        stages.stages_cleaned     AS application_stage,
        stages.is_milestone_stage,
        IFF(application_stage_name = 'Offer',offer_sent_date, stages.stage_entered_on) AS stage_entered_on,
        IFF(application_stage_name = 'Offer', offer_resolved_date, COALESCE(stages.stage_exited_on, CURRENT_DATE())) AS stage_exited_on,
        requisition_id,
        application_status        AS current_application_status,
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
        time_to_offer            AS app_to_offer_turntime
        FROM stages
        LEFT JOIN recruiting_xf 
        ON recruiting_xf.application_id = stages.application_id
    
    ), hired AS (

        SELECT 
        stages.application_id,
        candidate_id,
        'Hired'                                   AS application_stage,
        stages.is_milestone_stage,
        candidate_target_hire_date                AS stage_entered_on,
        candidate_target_hire_date                AS stage_exited_on,
        requisition_id,
        application_status                        AS current_application_status,
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
        time_to_offer                            AS app_to_offer_turntime
        FROM stages
        LEFT JOIN recruiting_xf 
        ON recruiting_xf.application_id = stages.application_id
        WHERE application_stage_name = 'Offer'
        AND application_status = 'hired' 
        AND is_hired_in_bamboo = TRUE 
    
    ), all_stages AS (
    
        SELECT * 
        FROM stages_intermediate
    
        UNION ALL

        SELECT *
        FROM hired

    ), stages_hit AS (

        SELECT 
        application_id,
        candidate_id,
        MIN(stage_entered_on)                                   AS min_stage_entered_on,
        MAX(stage_exited_on)                                    AS max_stage_exited_on,
        SUM(IFF(application_stage = 'Application Review',1,0))  AS hit_application_review,
        SUM(IFF(application_stage = 'Assessment',1,0))          AS hit_assessment,
        SUM(IFF(application_stage = 'Screen',1,0))              AS hit_screening,
        SUM(IFF(application_stage = 'Team Interview',1,0))      AS hit_team_interview,
        SUM(IFF(application_stage = 'Reference Check',1,0))     AS hit_reference_check,
        SUM(IFF(application_stage = 'Offer',1,0))               AS hit_offer,
        SUM(IFF(application_stage = 'Hired',1,0))               AS hit_hired
        FROM all_stages
        GROUP BY 1,2
        
    ), final AS (

        SELECT 
        {{ dbt_utils.surrogate_key('all_stages.application_id', 'all_stages.candidate_id') }} AS unique_key,
        all_stages.application_id,
        all_stages.candidate_id,
        application_stage,
        is_milestone_stage,
        DATE_TRUNC(MONTH,stage_entered_on) AS month_stage_entered_on,
        DATE_TRUNC(MONTH, stage_exited_on) AS month_stage_exited_on,
        lead(application_stage) OVER (PARTITION BY all_stages.application_id, all_stages.candidate_id ORDER BY stage_entered_on) AS next_stage,
        DATEDIFF(DAY, stage_entered_on, COALESCE(stage_exited_on, CURRENT_DATE())) AS days_in_stage,
        DATEDIFF(DAY, min_stage_entered_on, max_stage_exited_on) AS days_in_pipeline,
        requisition_id,
        current_application_status,
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
        app_to_offer_turntime,
        hit_application_review,
        hit_assessment,
        hit_screening,
        hit_team_interview,
        hit_reference_check,
        hit_offer,
        hit_hired,
        IFF(ROW_NUMBER() OVER (PARTITION BY all_stages.application_id, all_stages.candidate_id ORDER BY stage_entered_on DESC) =1, TRUE, FALSE) AS current_stage
        FROM all_stages
        LEFT JOIN stages_hit 
        ON all_stages.application_id = stages_hit.application_id
        AND all_stages.candidate_id = stages_hit.candidate_id
        
    )

    SELECT *
    FROM final