WITH source as (

	SELECT *
  	  FROM {{ source('greenhouse', 'jobs_offices') }}

), renamed as (

	SELECT

            --keys
            id::bigint              AS job_office_id,
            job_id::bigint          AS job_id,
            office_id::bigint       AS office_id,

            --info
            created_at::timestamp   AS job_office_created_at,
            updated_at::timestamp   AS job_office_updated_at

	FROM source

)

SELECT *
FROM renamed
