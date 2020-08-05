WITH source as (

	SELECT *
  	  FROM {{ source('greenhouse', 'jobs_interviews') }}

), renamed as (

	SELECT

            --keys
            id::bigint                  AS job_interview_id,
            job_id::bigint              AS job_id,
            stage_id::bigint            AS interview_stage_id,
            interview_id::bigint        AS interview_id,

            --info
            "order"::int                AS interview_order,
            estimated_duration::int     AS estimated_duration,
            created_at::timestamp       AS interview_created_at,
            updated_at::timestamp       AS interview_updated_at

	FROM source

)

SELECT *
FROM renamed
