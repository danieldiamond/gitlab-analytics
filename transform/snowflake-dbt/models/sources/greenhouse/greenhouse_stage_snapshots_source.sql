WITH source as (

	SELECT *
  	  FROM {{ source('greenhouse', 'stage_snapshots') }}

), renamed as (

	SELECT
            --keys
            stage_id::bigint    AS stage_snapshot_id,
            job_id::bigint      AS job_id,

            --info
            date::date          AS stage_snapshot_date,
            active_count::int   AS stage_snapshot_active_count
	FROM source

)

SELECT *
FROM renamed
