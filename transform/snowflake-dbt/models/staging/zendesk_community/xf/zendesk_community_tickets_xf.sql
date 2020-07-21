WITH zendesk_community_tickets AS (

  SELECT *
  FROM {{ref('zendesk_community_tickets_source')}}

), zendesk_community_ticket_metrics AS (

  SELECT
    ticket_id,
    solved_at,
    LEAST(sla_reply_time_business_hours, sla_reply_time_calendar_hours) AS first_reply_time
  FROM {{ref('zendesk_community_ticket_metrics')}}

), zendesk_community_sla_policies AS (

  SELECT DISTINCT 
    zendesk_community_sla_policy_id,
    zendesk_community_sla_title,
    policy_metrics_business_hours,
    policy_metrics_priority,
    policy_metrics_target
  FROM {{ref('zendesk_community_sla_policies_source')}}
  WHERE policy_metrics_metric = 'first_reply_time'

), zendesk_community_organizations AS (

  SELECT 
    organization_id,
    sfdc_account_id,
    organization_tags,
    organization_market_segment
  FROM {{ref('zendesk_community_organizations_source')}}

), zendesk_community_tickets_sla AS (

  SELECT *
  FROM {{ref('zendesk_community_tickets_sla_xf')}}

)

SELECT DISTINCT 
  zendesk_community_tickets.*,
  zendesk_community_ticket_metrics.first_reply_time,
  zendesk_community_organizations.sfdc_account_id,
  zendesk_community_organizations.organization_market_segment,
  zendesk_community_organizations.organization_tags,
  zendesk_community_tickets_sla.priority                              AS ticket_priority_at_first_reply,
  zendesk_community_tickets_sla.sla_policy                            AS ticket_sla_policy_at_first_reply,
  zendesk_community_tickets_sla.first_reply_time_sla,
  zendesk_community_tickets_sla.first_reply_at,
  zendesk_community_ticket_metrics.solved_at,
  IFF(zendesk_community_tickets_sla.first_reply_time_sla <= zendesk_community_sla_policies.policy_metrics_target, 
      True, False)                                          AS was_support_sla_met
FROM zendesk_community_tickets
LEFT JOIN zendesk_community_ticket_metrics
  ON zendesk_community_tickets.ticket_id = zendesk_community_ticket_metrics.ticket_id
LEFT JOIN zendesk_community_organizations
  ON zendesk_community_tickets.organization_id = zendesk_community_organizations.organization_id
LEFT JOIN zendesk_community_tickets_sla
  ON zendesk_community_tickets.ticket_id = zendesk_community_tickets_sla.ticket_id
LEFT JOIN zendesk_community_sla_policies
  ON zendesk_community_tickets_sla.priority = zendesk_community_sla_policies.policy_metrics_priority
  AND zendesk_community_tickets_sla.sla_policy = zendesk_community_sla_policies.zendesk_community_sla_title
