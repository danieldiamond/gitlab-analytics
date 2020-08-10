WITH source as (

	SELECT *
  	  FROM {{ source('greenhouse', 'scorecards') }}

), renamed as (

	SELECT

            --keys
            id::NUMBER                              AS scorecard_id,
            application_id::NUMBER                  AS application_id,
            stage_id::NUMBER                        AS stage_id,
            interview_id::NUMBER                    AS interview_id,
            interviewer_id::NUMBER                  AS interviewer_id,
            submitter_id::NUMBER                    AS submitter_id,

            --info
            overall_recommendation::varchar         AS scorecard_overall_recommendation,
            submitted_at::timestamp                 AS scorecard_submitted_at,
            scheduled_interview_ended_at::timestamp AS scorecard_scheduled_interview_ended_at,
            total_focus_attributes::NUMBER             AS scorecard_total_focus_attributes,
            completed_focus_attributes::NUMBER         AS scorecard_completed_focus_attributes,
            stage_name::varchar                     AS scorecard_stage_name,
            created_at::timestamp                   AS scorecard_created_at,
            updated_at::timestamp                   AS scorecard_updated_at,
            interview_name::varchar                 AS interview_name,
            interviewer::varchar                    AS interviewer,
            submitter::varchar                      AS scorecard_submitter

	FROM source

)

SELECT *
FROM renamed
