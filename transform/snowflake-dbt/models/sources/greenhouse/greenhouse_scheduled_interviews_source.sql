WITH source as (

	SELECT *
  	  FROM {{ source('greenhouse', 'scheduled_interviews') }}

), renamed as (

	SELECT

            --keys
            id::NUMBER                  AS scheduled_interview_id,
            application_id::NUMBER      AS application_id,
            interview_id::NUMBER        AS interview_id,
            scheduled_by_id::NUMBER     AS interview_scheduled_by_id,

            --info
            status::varchar             AS scheduled_interview_status,
            scheduled_at::timestamp     AS interview_scheduled_at,
            starts_at::timestamp        AS interview_starts_at,
            ends_at::timestamp          AS interview_ends_at,
            all_day_start_date::varchar::date    AS all_day_start_date,
            all_day_end_date::varchar::date      AS all_day_end_date,
            stage_name::varchar         AS scheduled_interview_stage_name,
            interview_name::varchar     AS scheduled_interview_name


	FROM source

)

SELECT *
FROM renamed
