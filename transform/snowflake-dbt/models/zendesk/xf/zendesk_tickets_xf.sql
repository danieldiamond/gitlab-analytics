WITH zendesk_tickets AS (

  SELECT *
  FROM {{ref('zendesk_tickets')}}

), zendesk_ticket_metrics AS (

  SELECT
    ticket_id,
    solved_at,
    LEAST(sla_reply_time_business_hours, sla_reply_time_calendar_hours) AS first_reply_time
  FROM {{ref('zendesk_ticket_metrics')}}

), zendesk_sla_policies AS (

  SELECT DISTINCT 
    zendesk_sla_policy_id,
    zendesk_sla_title,
    policy_metrics_business_hours,
    policy_metrics_priority,
    policy_metrics_target
  FROM {{ref('zendesk_sla_policies')}}
  WHERE policy_metrics_metric = 'first_reply_time'

), zendesk_organizations AS (

  SELECT 
    organization_id,
    sfdc_account_id,
    organization_tags,
    organization_market_segment
  FROM {{ref('zendesk_organizations')}}

), zendesk_tickets_sla AS (

  SELECT *
  FROM {{ref('zendesk_tickets_sla_xf')}}

)

SELECT DISTINCT 
  zendesk_tickets.*,
  zendesk_ticket_metrics.first_reply_time,
  zendesk_organizations.sfdc_account_id,
  zendesk_organizations.organization_market_segment,
  zendesk_organizations.organization_tags,
  zendesk_tickets_sla.priority                              AS ticket_priority_at_first_reply,
  zendesk_tickets_sla.sla_policy                            AS ticket_sla_policy_at_first_reply,
  zendesk_tickets_sla.first_reply_time_sla,
  zendesk_tickets_sla.first_reply_at,
  zendesk_ticket_metrics.solved_at,
  IFF(zendesk_tickets_sla.first_reply_time_sla <= zendesk_sla_policies.policy_metrics_target, 
      True, False)                                          AS was_support_sla_met
FROM zendesk_tickets
LEFT JOIN zendesk_ticket_metrics
  ON zendesk_tickets.ticket_id = zendesk_ticket_metrics.ticket_id
LEFT JOIN zendesk_organizations
  ON zendesk_tickets.organization_id = zendesk_organizations.organization_id
LEFT JOIN zendesk_tickets_sla
  ON zendesk_tickets.ticket_id = zendesk_tickets_sla.ticket_id
LEFT JOIN zendesk_sla_policies
  ON zendesk_tickets_sla.priority = zendesk_sla_policies.policy_metrics_priority
  AND zendesk_tickets_sla.sla_policy = zendesk_sla_policies.zendesk_sla_title
