WITH source as (

    SELECT *
    FROM {{ source('greenhouse', 'candidates') }}

), renamed as (

    SELECT
            --keys
            id::bigint              AS candidate_id,
            recruiter_id::bigint    AS candidate_recruiter_id,
            coordinator_id::bigint  AS candidate_coordinator_id,

            --info
            recruiter::varchar      AS candidate_recruiter,
            coordinator::varchar    AS candidate_coordinator,
            company::varchar        AS candidate_company,
            title::varchar          AS candidate_title,
            created_at::timestamp   AS candidate_created_at,
            updated_at::timestamp   AS candidate_updated_at,
            migrated::boolean       AS is_candidate_migrated,
            private::boolean        AS is_candidate_private

    FROM source

)

SELECT *
FROM renamed
