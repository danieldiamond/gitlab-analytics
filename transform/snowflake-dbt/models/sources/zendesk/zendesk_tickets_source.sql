WITH source AS (

    SELECT *
    FROM {{ source('zendesk', 'tickets') }}
),

renamed AS (

    SELECT
        id                                      AS ticket_id,
        created_at                              AS ticket_created_at,
        --ids
        organization_id,
        assignee_id,
        brand_id,
        group_id,
        requester_id,
        submitter_id,

        --fields
        status                                  AS ticket_status,
        lower(priority)                         AS ticket_priority,
        md5(subject)                            AS ticket_subject,
        md5(recipient)                          AS ticket_recipient,
        url                                     AS ticket_url,
        tags                                    AS ticket_tags,
        -- added ':score'
        satisfaction_rating['score']::VARCHAR   AS satisfaction_rating_score,
        via['channel']::VARCHAR                 AS submission_channel,

        --dates
        updated_at::DATE                        AS date_updated

    FROM source

)

SELECT *
FROM renamed
