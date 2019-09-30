{{config({
    "schema": "staging"
  })
}}

with source as (

    SELECT *
    FROM {{ source('zendesk', 'tickets') }}
),

renamed as (

    SELECT
        id                                   AS ticket_id,
        created_at                           AS ticket_created_at,
        --ids
        organization_id,
        assignee_id,
        brand_id,
        group_id,
        requester_id,
        submitter_id,

        --fields
        status                                AS ticket_status,
        lower(priority)                       AS ticket_first_priority,
        md5(subject)                          AS ticket_subject,
        md5(recipient)                        AS ticket_recipient,
        url                                   AS ticket_url,
        tags                                  AS ticket_tags,
        -- added ':score'
        satisfaction_rating['score']::varchar AS satisfaction_rating_score,
        via['channel']::varchar               AS submission_channel,

        --dates
        updated_at::date                      AS date_updated

    FROM source

)

SELECT *
FROM renamed
