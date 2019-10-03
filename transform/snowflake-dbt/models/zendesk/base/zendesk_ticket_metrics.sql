{{config({
    "schema": "staging"
  })
}}

WITH source AS (

    SELECT *
    FROM {{ source('zendesk', 'ticket_metrics') }}

),

renamed AS (

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
        /* The following is a stopgap solution to set the minimum value between first resolution,
           full resolution and reply time for calendar and business hours respectively.
           In Snowflake, LEAST will always return NULL if any input to the function is NULL.
           In the event all three metrics are NULL, NULL should be returned */
        IFF(
            first_resolution_time_in_minutes_during_calendar_hours IS NULL
              AND full_resolution_time_in_minutes_during_calendar_hours IS NULL
              AND reply_time_in_minutes_during_calendar_hours IS NULL,
            NULL,
            LEAST(
              COALESCE(first_resolution_time_in_minutes_during_calendar_hours, 50000000),
              -- 50,000,000 is roughly 100 years, sufficient to not be the LEAST value
              COALESCE(full_resolution_time_in_minutes_during_calendar_hours, 50000000),
              COALESCE(reply_time_in_minutes_during_calendar_hours, 50000000)
            )
        )                                                   AS sla_reply_time_calendar_hours,
        IFF(
            first_resolution_time_in_minutes_during_business_hours IS NULL
              AND full_resolution_time_in_minutes_during_business_hours IS NULL
              AND reply_time_in_minutes_during_business_hours IS NULL,
            NULL,
            LEAST(
              COALESCE(first_resolution_time_in_minutes_during_business_hours, 50000000),
              COALESCE(full_resolution_time_in_minutes_during_business_hours, 50000000),
              COALESCE(reply_time_in_minutes_during_business_hours, 50000000)
            )
        )                                                   AS sla_reply_time_business_hours,
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
