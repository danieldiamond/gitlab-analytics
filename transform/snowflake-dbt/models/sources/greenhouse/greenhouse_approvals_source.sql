WITH source as (

	SELECT *
  	  FROM {{ source('greenhouse', 'approvals') }}

), renamed as (

	SELECT

            --keys
            offer_id::bigint            AS offer_id,
            application_id::bigint      AS application_id,
            job_id::bigint              AS job_id,
            candidate_id::bigint        AS candidate_id,
            approver_id::bigint         AS approver_id,
            group_id::bigint            AS group_id,

            --info
            approval_type::varchar      AS approval_type,
            status::varchar             AS approval_status,
            version::int                AS approval_version,
            final_version::int          AS approval_version_final,
            group_order::int            AS group_order,
            group_quorum::int           AS group_quorum,
            assigned::timestamp         AS approval_assigned_at,
            completed::timestamp        AS approval_completed_at,
            created_at::timestamp       AS approval_created_at,
            updated_at::timestamp       AS approval_updated_at

	FROM source

)

SELECT *
FROM renamed
