with source as (

    SELECT *
    FROM {{ source('zendesk', 'ticket_metrics') }}

),

renamed as (

    SELECT

        --ids
        id,
        ticket_id,

        --fields
        agent_wait_time_in_minutes:business                                                 AS agent_wait_time_in_minutes_business_hours,
        agent_wait_time_in_minutes:calendar                                                 AS agent_wait_time_in_minutes_calendar_hours,
        first_resolution_time_in_minutes:business                                           AS first_resolution_time_in_minutes_during_business_hours,
        first_resolution_time_in_minutes:calendar                                           AS first_resolution_time_in_minutes_during_calendar_hours,
        full_resolution_time_in_minutes:business                                            AS full_resolution_time_in_minutes_during_business_hours,
        full_resolution_time_in_minutes:calendar                                            AS full_resolution_time_in_minutes_during_calendar_hours,
        on_hold_time_in_minutes:business                                                    AS on_hold_time_in_minutes_during_business_hours,
        on_hold_time_in_minutes:calendar                                                    AS on_hold_time_in_minutes_during_calendar_hours,
        reopens,
        replies                                                                             AS total_replies,
        reply_time_in_minutes:business                                                      AS reply_time_in_minutes_during_business_hours,
        reply_time_in_minutes:calendar                                                      AS reply_time_in_minutes_during_calendar_hours,
        requester_wait_time_in_minutes:business                                             AS requester_wait_time_in_minutes_during_business_hours,
        requester_wait_time_in_minutes:calendar                                             AS requester_wait_time_in_minutes_during_calendar_hours,
        assignee_stations                                                                   AS assignee_station_number,
        group_stations                                                                      AS group_station_number,
        
        --dates
        created_at                                                                          AS creation_date,
        assigned_at                                                                         AS latest_assigned_at,
        initially_assigned_at,
        latest_comment_added_at,
        solved_at                                                                           AS solved_date,
        updated_at,
        assignee_updated_at

FROM source

)

SELECT * 
FROM renamed