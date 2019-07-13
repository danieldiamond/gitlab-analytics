{{ config({
    "schema": "analytics",
    "post-hook": "grant select on {{this}} to role reporter"
    })
}}

WITH zendesk_tickets AS (

  SELECT * FROM {{ref('zendesk_tickets')}}

), zendesk_ticket_metrics AS (

  SELECT ticket_id,
          first_reply_time
  FROM {{ref('zendesk_ticket_metrics')}}

), zendesk_organizations AS (

  SELECT organization_id,
         organization_name,
         sfdc_account_id,
         organization_market_segment
  FROM {{ref('zendesk_organizations')}}

)

SELECT zendesk_tickets.*,
       zendesk_organizations.sfdc_account_id,
       zendesk_organizations.organization_market_segment,
       zendesk_ticket_metrics.first_reply_time,
      {{ support_sla_met( 'first_reply_time',
                          'ticket_priority',
                          'ticket_created_at') }} AS was_support_sla_met
FROM zendesk_tickets
LEFT JOIN zendesk_ticket_metrics
  ON zendesk_ticket_metrics.ticket_id = zendesk_tickets.ticket_id
LEFT JOIN zendesk_organizations
  ON zendesk_organizations.organization_id = zendesk_tickets.organization_id
