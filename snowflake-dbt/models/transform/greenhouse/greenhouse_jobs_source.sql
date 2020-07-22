WITH source as (

	SELECT *
  	  FROM {{ source('greenhouse', 'jobs') }}

), renamed as (

	SELECT

			--keys
    		id::bigint							    AS job_id,
    		organization_id::bigint 		AS organization_id,
    		requisition_id::varchar			AS requisition_id,
    		department_id::bigint				AS department_id,

    		--info
    		name::varchar						    AS job_name,
    		status::varchar					    AS job_status,
    		opened_at::timestamp				AS job_opened_at,
    		closed_at::timestamp				AS job_closed_at,
    		level::varchar 					    AS job_level,
    		confidential::boolean				AS is_confidential,
    		created_at::timestamp				AS job_created_at,
    		notes::varchar						  AS job_notes,
    		updated_at::timestamp				AS job_updated_at


	FROM source

)

SELECT *
FROM renamed
