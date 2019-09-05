{{ config({
    "schema": "analytics"
    })
}}


WITH source as (

	SELECT *
  	FROM {{ source('greenhouse', 'applications') }}

), renamed as (

	SELECT  id 						        AS application_id,

			--keys
		    candidate_id,
		    stage_id,
		    source_id,
		    referrer_id,
		    rejected_by_id,
		    job_post_id,
		    event_id,
		    rejection_reason_id,
		    converted_prospect_application_id,

		    --info
		    status 					        AS application_status,
		    prospect,

		    pipeline_percent,
		    migrated,
		    rejected_by,
		    stage_name,
		    prospect_pool,
		    prospect_pool_stage,

		    applied_at::timestamp 	AS applied_at,
		    rejected_at::timestamp 	AS rejected_at,
		    created_at::timestamp 	AS created_at,
		    updated_at::timestamp 	AS last_updated_at

	FROM source

)

SELECT *
FROM renamed
