WITH source as (

	SELECT *
  	  FROM {{ source('greenhouse', 'jobs_attributes') }}

), renamed as (

	SELECT

			--keys
    		id::bigint				    AS job_attribute_id,
    		job_id::bigint			  AS job_id,
    		attribute_id::bigint	AS attribute_id,

    		--info
    		active::boolean			  AS is_active,
    		created_at::timestamp	AS jobs_attribute_created_at,
    		updated_at::timestamp	AS jobs_attribute_updated_at

	FROM source

)

SELECT *
FROM renamed
