with source as (

    SELECT *
    FROM {{ var("database") }}.zendesk_stitch.ticket_metrics

),

renamed AS (

    SELECT

        --ids
        id,
        ticket_id,

        --fields
        agent_wait_time_in_minutes:business                 as agent_wait_time_in_minutes_business_hours,
        agent_wait_time_in_minutes:calendar                 as agent_wait_time_in_minutes_calendar_hours,
        first_resolution_time_in_minutes:business           as first_resolution_time_in_minutes_during_business_hours,
        first_resolution_time_in_minutes:calendar           as first_resolution_time_in_minutes_during_calendar_hours,
        full_resolution_time_in_minutes:business            as full_resolution_time_in_minutes_during_business_hours,
        full_resolution_time_in_minutes:calendar            as full_resolution_time_in_minutes_during_calendar_hours,
        on_hold_time_in_minutes:business                    as on_hold_time_in_minutes_during_business_hours,
        on_hold_time_in_minutes:calendar                    as on_hold_time_in_minutes_during_calendar_hours,
        reopens,
        replies                                             as total_replies,
        reply_time_in_minutes:business                      as reply_time_in_minutes_during_business_hours,
        reply_time_in_minutes:calendar                      as reply_time_in_minutes_during_calendar_hours,
        requester_wait_time_in_minutes:business             as requester_wait_time_in_minutes_during_business_hours,
        requester_wait_time_in_minutes:calendar             as requester_wait_time_in_minutes_during_calendar_hours,
        assignee_stations                                   as assignee_station_number,
        group_stations                                      as group_station_number,
        
        --dates
        created_at                                          as creation_date,
        assigned_at                                         as latest_assigned_at,
        initially_assigned_at,
        latest_comment_added_at,
        solved_at                                           as solved_date,
        updated_at,
        assignee_updated_at

FROM source

)

SELECT * 
FROM renamed