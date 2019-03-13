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
        agent_wait_time_in_minutes:business                 as agent_wait_time_in_minutes_business,
        agent_wait_time_in_minutes:calendar                 as agent_wait_time_in_minutes_calendar,
        first_resolution_time_in_minutes:business           as first_resolution_time_in_minutes_business,
        first_resolution_time_in_minutes:calendar           as first_resolution_time_in_minutes_calendar,
        full_resolution_time_in_minutes:business            as full_resolution_time_in_minutes_business,
        full_resolution_time_in_minutes:calendar            as full_resolution_time_in_minutes_calendar,
        on_hold_time_in_minutes:business                    as on_hold_time_in_minutes_business,
        on_hold_time_in_minutes:calendar                    as on_hold_time_in_minutes_calendar,
        reopens,
        replies,
        reply_time_in_minutes:business                      as reply_time_in_minutes_business,
        reply_time_in_minutes:calendar                      as reply_time_in_minutes_calendar,
        requester_wait_time_in_minutes:business             as requester_wait_time_in_minutes_business,
        requester_wait_time_in_minutes:calendar             as requester_wait_time_in_minutes_calendar,
        assignee_stations                                   as assignee_station_number,
        group_stations                                      as group_station_number,
        
        --dates
        created_at,
        assigned_at                                         as latest_assigned_at,
        initially_assigned_at,
        latest_comment_added_at,
        solved_at,
        updated_at,
        assignee_updated_at

FROM source

)

SELECT * 
FROM renamed