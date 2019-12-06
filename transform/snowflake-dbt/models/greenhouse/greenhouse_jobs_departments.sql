WITH source as (

	SELECT *
  	  FROM {{ source('greenhouse', 'jobs_departments') }}

), renamed as (

	SELECT

			--keys
    		id::bigint				      AS job_department_id,
    		job_id::bigint			    AS job_id,
    		department_id::bigint	  AS department_id,

    		--info
    		created_at::timestamp 	AS job_department_created_at,
    		updated_at::timestamp 	AS job_department_updated_at


	FROM source

)

SELECT *
FROM renamed
