WITH source as (

	SELECT *
  	  FROM {{ source('greenhouse', 'openings') }}

), renamed as (

	SELECT
            --keys
            id::bigint                      AS job_opening_id,
            job_id::bigint                  AS job_id,
            opening_id::varchar             AS opening_id,
            hired_application_id::bigint    AS hired_application_id,

            --info
            opened_at::timestamp            AS job_opened_at,
            closed_at::timestamp            AS job_closed_at,
            close_reason::varchar           AS close_reason,
            created_at::timestamp           AS job_opening_created_at,
            updated_at::timestamp           AS job_opening_updated_at,
            target_start_date::date         AS target_start_date

	FROM source

)

SELECT *
FROM renamed
