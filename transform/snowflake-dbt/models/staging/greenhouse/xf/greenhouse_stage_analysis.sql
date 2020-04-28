{{ config({
    "materialized":"table",
    "schema": "analytics"
    })
}}

 WITH greenhouse_stage_intermediate AS (

    SELECT *
    FROM {{ ref ('greenhouse_stage_intermediate') }}
 
 ), final AS (

    SELECT
      unique_key,
      application_stage, 
      is_milestone_stage,
      DATE_TRUNC(week, stage_entered_on) AS week_stage_entered_on,
      DATE_TRUNC(week, stage_exited_on) AS week_stage_exited_on,
      month_stage_entered_on,
      month_stage_exited_on,
      days_in_stage,
      days_in_pipeline,
      row_number_stages_desc,
      next_stage,
      is_current_stage,
      application_month,
      requisition_id,
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
      time_to_offer,
      hit_application_review,
      hit_assessment,
      hit_screening,
      hit_team_interview,
      hit_reference_check,
      hit_offer,
      hit_hired,
      hit_rejected
    FROM greenhouse_stage_intermediate

 )

 SELECT *
 FROM final      