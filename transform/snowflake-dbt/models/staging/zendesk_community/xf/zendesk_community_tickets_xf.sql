WITH zendesk_community_tickets AS (

  SELECT *
  FROM {{ref('zendesk_community_tickets_source')}}

), zendesk_community_ticket_metrics AS (

  SELECT
    ticket_id,
    solved_at,
    LEAST(sla_reply_time_business_hours, sla_reply_time_calendar_hours) AS first_reply_time
  FROM {{ref('zendesk_community_ticket_metrics')}}

), zendesk_community_organizations AS (
  SELECT
    organization_id,
    sfdc_account_id,
    organization_tags,
    organization_market_segment
  FROM {{ref('zendesk_community_organizations_source')}}
)

SELECT DISTINCT 
  zendesk_community_tickets.*,
  zendesk_community_ticket_metrics.first_reply_time,
  zendesk_community_organizations.sfdc_account_id,
  zendesk_community_organizations.organization_market_segment,
  zendesk_community_organizations.organization_tags,
  zendesk_community_ticket_metrics.solved_at
FROM zendesk_community_tickets
LEFT JOIN zendesk_community_ticket_metrics
  ON zendesk_community_tickets.ticket_id = zendesk_community_ticket_metrics.ticket_id
LEFT JOIN zendesk_community_organizations
  ON zendesk_community_tickets.organization_id = zendesk_community_organizations.organization_id
