WITH source as (

	SELECT *
  	FROM {{ source('greenhouse', 'applications_jobs') }}

), renamed as (

	SELECT
			--keys
    		application_id::bigint		AS application_id,
    		job_id::bigint				    AS job_id

	FROM source

)

SELECT *
FROM renamed
