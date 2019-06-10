{{ config({
    "schema": "analytics",
    "post-hook": "grant select on {{this}} to role reporter"
    })
}}

WITH zendesk_tickets AS (

  SELECT *
  FROM {{ref('zendesk_tickets')}}
  WHERE zendesk_tickets.ticket_priority IS NOT NULL

), zendesk_ticket_metrics AS (

  SELECT *
  FROM {{ref('zendesk_ticket_metrics')}}

), zendesk_organizations AS (

  SELECT organization_id,
         name,
         arr,
         organization_market_segment,
         sfdc_id
  FROM {{ref('zendesk_organizations')}}

)

SELECT zendesk_tickets.assignee_id,
       zendesk_tickets.brand_id,
       zendesk_tickets.group_id,
       zendesk_tickets.requester_id,
       zendesk_tickets.submitter_id,
       zendesk_tickets.ticket_status,
       zendesk_tickets.ticket_priority,
       zendesk_tickets.ticket_subject,
       zendesk_tickets.ticket_recipient,
       zendesk_tickets.ticket_type,
       zendesk_tickets.api_url,
       zendesk_tickets.satisfaction_rating_score,
       zendesk_tickets.date_created,
       zendesk_tickets.date_updated,                                                             
       zendesk_ticket_metrics.*,
       zendesk_organizations.*,                                                                        
  COALESCE(reply_time_in_minutes_during_calendar_hours, first_resolution_time_in_minutes_during_calendar_hours, full_resolution_time_in_minutes_during_calendar_hours ) AS first_reply_time,
  EXTRACT(dow FROM creation_date)                                                                               AS day_of_week_submitted,
  EXTRACT(hour FROM creation_date)                                                                              AS hour_submitted,
  DATE_TRUNC('day', DATEADD(day, 1, creation_date))                                                             AS day_end,
  DATEDIFF(mins, creation_date, day_end)                                                                        AS minutes_before_day_end,
  CASE
    -- Logic for urgent tickets with a 24/7 SLA of 30 minutes
    WHEN first_reply_time <= 30  
        AND ticket_priority = 'urgent' THEN 'Met'
    -- Logic for high priority tickets with a 24/5 SLA of 4 hours or 240 minutes
    WHEN first_reply_time <= 240 
        AND ticket_priority = 'high'  THEN 'Met'
    -- Logic for tickets submitted on a Friday after 8 pm. Minutes remaining in day +  minutes elapsed over the weekend (2880) + unused SLA minutes
    WHEN ticket_priority = 'high' AND day_of_week_submitted = 5 
        AND hour_submitted >= 20
        AND first_reply_time <= minutes_before_day_end + 2880 + (240 - minutes_before_day_end)
        THEN 'Met'
    -- Logic for high priority tickets submitted over the weekend. Minutes elapsed over the weekend (2880) + allotted SLA minutes
    WHEN ticket_priority = 'high' AND day_of_week_submitted = 6 OR day_of_week_submitted = 0 AND first_reply_time <= 2880 + 240
        THEN 'Met'
    -- Logic for normal priority tickets with a 24/5 SLA of 8 hours or 480 minutes
    WHEN first_reply_time <= 480 
        AND ticket_priority = 'normal' THEN 'Met'
    -- Logic for normal priority tickets submitted on a Friday after 4 pm. Minutes remaining in day +  minutes elapsed over the weekend (2880) + Unused SLA minutes
    WHEN ticket_priority = 'normal' AND day_of_week_submitted = 5 AND hour_submitted >= 16
        AND first_reply_time <= minutes_before_day_end + 2880 + (480 - minutes_before_day_end)
        THEN 'Met'
    -- Logic for normal priority tickets submitted over the weekend. Minutes elapsed over the weekend + allotted SLA minutes
    WHEN ticket_priority = 'normal' AND day_of_week_submitted = 6 OR day_of_week_submitted = 0 AND first_reply_time <= 2880 + 480
        THEN 'Met'
    -- Logic for low priority tickets with a 24/5 SLA of 24 hours or 1440 minutes
    WHEN first_reply_time <= 1440 AND ticket_priority = 'low' THEN 'Met'
    -- Logic for low priority tickets submitted on a Friday after 12am. Minutes remaining in day + minutes elapsed over the weekend (2880) + unused SLA minutes
    WHEN ticket_priority = 'low' AND day_of_week_submitted = 5 AND hour_submitted > 0
        AND first_reply_time <= minutes_before_day_end + 2880 + (1440 - minutes_before_day_end)
        THEN 'Met'
    -- Logic for low priority tickets submitted over the weekend. Minutes elapsed over the weekend (2880) + allotted SLA minutes
    WHEN ticket_priority = 'low' AND day_of_week_submitted = 6 OR day_of_week_submitted = 0 AND first_reply_time <= 2880 + 1440
        THEN 'Met'
    ELSE NULL
    END                                                                                                           AS sla_status
FROM zendesk_tickets
INNER JOIN zendesk_ticket_metrics
  ON zendesk_ticket_metrics.ticket_id = zendesk_tickets.ticket_id
INNER JOIN zendesk_organizations
  ON zendesk_organizations.organization_id = zendesk_tickets.organization_id
