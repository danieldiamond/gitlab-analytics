{{ config({
    "schema": "analytics",
    "post-hook": "grant select on {{this}} to role reporter"
    })
}}

WITH zendesk_tickets AS (

  SELECT *
  FROM {{ref('zendesk_tickets')}}
  WHERE NOT zendesk_tickets.satisfaction_rating_score = 'unoffered' 
  AND NOT zendesk_tickets.satisfaction_rating_score = 'offered'


), zendesk_org AS (

  SELECT *
  FROM {{ref('zendesk_organizations')}}

)

SELECT  zendesk_org.organization_id,                       
        zendesk_org.organization_market_segment,                      
        zendesk_tickets.ticket_priority,                   
        zendesk_tickets.ticket_recipient,                         
        zendesk_tickets.ticket_status,                            
        zendesk_tickets.satisfaction_rating_score,
        zendesk_tickets.date_created
FROM zendesk_tickets
INNER JOIN zendesk_org
  ON zendesk_tickets.organization_id = zendesk_org.organization_id