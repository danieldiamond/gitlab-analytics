WITH zendesk_community_relations_tickets AS (

  SELECT *
  FROM {{ref('zendesk_community_relations_tickets_source')}}

), zendesk_community_relations_ticket_metrics AS (

  SELECT
    ticket_id,
    solved_at,
    sla_reply_time_business_hours,
    sla_reply_time_calendar_hours
  FROM {{ref('zendesk_community_relations_ticket_metrics')}}

), zendesk_community_relations_organizations AS (
  SELECT
    organization_id,
    sfdc_account_id,
    organization_tags,
    organization_market_segment
  FROM {{ref('zendesk_community_relations_organizations_source')}}
)

SELECT DISTINCT 
  zendesk_community_relations_tickets.*,
  zendesk_community_relations_ticket_metrics.sla_reply_time_business_hours,
  zendesk_community_relations_ticket_metrics.sla_reply_time_calendar_hours,
  zendesk_community_relations_organizations.sfdc_account_id,
  zendesk_community_relations_organizations.organization_market_segment,
  zendesk_community_relations_organizations.organization_tags,
  zendesk_community_relations_ticket_metrics.solved_at
FROM zendesk_community_relations_tickets
LEFT JOIN zendesk_community_relations_ticket_metrics
  ON zendesk_community_relations_tickets.ticket_id = zendesk_community_relations_ticket_metrics.ticket_id
LEFT JOIN zendesk_community_relations_organizations
  ON zendesk_community_relations_tickets.organization_id = zendesk_community_relations_organizations.organization_id
