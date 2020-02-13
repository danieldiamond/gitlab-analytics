WITH source as (

	SELECT *
  	  FROM {{ source('greenhouse', 'job_snapshots') }}

), renamed as (

	SELECT

			--key
    		job_id::bigint			AS job_id,

    		--info
    		date::date			    AS job_snapshot_date,
    		hired_count::int 		AS hired_count,
    		prospect_count::int AS prospect_count,
    		new_today::int 			AS new_today,
    		rejected_today::int AS rejected_today,
    		advanced_today::int AS advanced_today,
    		interviews_today::int AS interviews_today

	FROM source

)

SELECT *
FROM renamed
