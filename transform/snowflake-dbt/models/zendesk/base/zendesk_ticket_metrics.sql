{{config({
    "schema": "staging"
  })
}}

with source as (

    SELECT *
    FROM {{ source('zendesk', 'ticket_metrics') }}

),

renamed as (

    SELECT

        --ids
        id                                                  AS ticket_metrics_id,
        ticket_id,

        --fields
        agent_wait_time_in_minutes['business']::float       AS agent_wait_time_in_minutes_business_hours,
        agent_wait_time_in_minutes['calendar']::float       AS agent_wait_time_in_minutes_calendar_hours,
        first_resolution_time_in_minutes['business']::float AS first_resolution_time_in_minutes_during_business_hours,
        first_resolution_time_in_minutes['calendar']::float AS first_resolution_time_in_minutes_during_calendar_hours,
        full_resolution_time_in_minutes['business']::float  AS full_resolution_time_in_minutes_during_business_hours,
        full_resolution_time_in_minutes['calendar']::float  AS full_resolution_time_in_minutes_during_calendar_hours,
        on_hold_time_in_minutes['business']::float          AS on_hold_time_in_minutes_during_business_hours,
        on_hold_time_in_minutes['calendar']::float          AS on_hold_time_in_minutes_during_calendar_hours,
        reopens,
        replies                                             AS total_replies,
        reply_time_in_minutes['business']::float            AS reply_time_in_minutes_during_business_hours,
        reply_time_in_minutes['calendar']::float            AS reply_time_in_minutes_during_calendar_hours,
        requester_wait_time_in_minutes['business']::float   AS requester_wait_time_in_minutes_during_business_hours,
        requester_wait_time_in_minutes['calendar']::float   AS requester_wait_time_in_minutes_during_calendar_hours,
        assignee_stations                                   AS assignee_station_number,
        group_stations                                      AS group_station_number,

        COALESCE(reply_time_in_minutes_during_calendar_hours,
                first_resolution_time_in_minutes_during_calendar_hours,
                full_resolution_time_in_minutes_during_calendar_hours) AS first_reply_time,
        --dates
        created_at,
        assigned_at,
        initially_assigned_at,
        latest_comment_added_at,
        solved_at,
        updated_at,
        assignee_updated_at

    FROM source

)

SELECT *
FROM renamed
