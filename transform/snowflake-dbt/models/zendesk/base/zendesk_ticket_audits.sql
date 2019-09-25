{{config({
    "schema": "staging"
  })
}}

with source as (

    SELECT *
    FROM {{ source('zendesk', 'ticket_audits') }}
),

renamed as (

    SELECT
        id                                   AS audit_id,
        created_at                           AS audit_created_at,

        --ids
        ticket_id                            AS ticket_id,
        author_id                            AS author_id,

        --fields
        event                                AS audit_event,
        metadata                             AS audit_metadata,
        via                                  AS audit_via,

    FROM source

)

SELECT *
FROM renamed
