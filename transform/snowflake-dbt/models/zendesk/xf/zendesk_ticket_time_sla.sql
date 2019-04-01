{{ config(schema='analytics') }}

WITH zendesk_tickets AS (

  SELECT *
  FROM {{ref('zendesk_tickets')}}
  WHERE zendesk_tickets.ticket_status  = 'closed'

), zendesk_ticket_metrics AS (

  SELECT *
  FROM {{ref('zendesk_ticket_metrics')}}

), zendesk_organizations AS (

  SELECT *
  FROM {{ref('zendesk_organizations')}}

)



SELECT  zendesk_tickets.organization_id,                                                 
        zendesk_organizations.sfdc_id,                                                  
        zendesk_tickets.ticket_status,                                                   
        zendesk_ticket_metrics.creation_date,                                             
        zendesk_ticket_metrics.solved_date,
        zendesk_ticket_metrics.total_replies,                                                  
        zendesk_ticket_metrics.first_resolution_time_in_minutes_during_business_hours,   
        zendesk_ticket_metrics.full_resolution_time_in_minutes_during_business_hours,    
        full_resolution_time_in_minutes_during_business_hours - first_resolution_time_in_minutes_during_business_hours   AS time_between_resolutions_during_business_hours,
        zendesk_ticket_metrics.first_resolution_time_in_minutes_during_calendar_hours,   
        zendesk_ticket_metrics.full_resolution_time_in_minutes_during_calendar_hours,    
        full_resolution_time_in_minutes_during_calendar_hours - first_resolution_time_in_minutes_during_calendar_hours    AS time_between_resolutions_during_calendar_hours
FROM zendesk_tickets
INNER JOIN zendesk_ticket_metrics
  ON zendesk_ticket_metrics.ticket_id = zendesk_tickets.ticket_id
INNER JOIN zendesk_organizations                                                
  ON zendesk_organizations.organization_id = zendesk_tickets.organization_id

