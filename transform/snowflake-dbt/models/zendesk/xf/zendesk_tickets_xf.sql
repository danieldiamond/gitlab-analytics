WITH zendesk_tickets AS (

  SELECT * FROM {{ref('zendesk_tickets')}}

), zendesk_ticket_metrics AS (

  SELECT ticket_id,
          first_reply_time,
          reply_time_in_minutes_during_business_hours,
          reply_time_in_minutes_during_calendar_hours
  FROM {{ref('zendesk_ticket_metrics')}}

), zendesk_sla_policies AS (

  SELECT *
  FROM {{ref('zendesk_sla_policies')}}

), zendesk_organizations AS (

  SELECT organization_id,
         organization_name,
         sfdc_account_id,
         organization_tags,
         CASE WHEN lower(organization_tags) LIKE '%premium%'
              OR lower(organization_tags) LIKE '%ultimate%'
              THEN 166808
          ELSE NULL END AS has_premium_ultimate_sla,
         organization_market_segment
  FROM {{ref('zendesk_organizations')}}

)

SELECT zendesk_tickets.*,
       zendesk_organizations.sfdc_account_id,
       zendesk_organizations.organization_market_segment,
       zendesk_organizations.organization_tags,
       COALESCE(has_emergency_sla, has_premium_ultimate_sla) AS zendesk_sla_policy_id,
       CASE WHEN zendesk_sla_policies.policy_metrics_business_hours = True THEN reply_time_in_minutes_during_business_hours
            WHEN zendesk_sla_policies.policy_metrics_business_hours = Fasle THEN reply_time_in_minutes_during_calendar_hours
            END AS first_reply_time,
       zendesk_sla_policies.policy_metrics_target
FROM zendesk_tickets
LEFT JOIN zendesk_ticket_metrics
  ON zendesk_ticket_metrics.ticket_id = zendesk_tickets.ticket_id
LEFT JOIN zendesk_organizations
  ON zendesk_organizations.organization_id = zendesk_tickets.organization_id
LEFT JOIN zendesk_sla_policies
  ON (zendesk_sla_policies.zendesk_sla_policy_id = zendesk_tickets.has_emergency_sla
    AND zendesk_tickets.ticket_priority = zendesk_sla_policies.policy_metrics_priority)
  OR (zendesk_sla_policies.zendesk_sla_policy_id = zendesk_organizations.has_premium_ultimate_sla
    AND zendesk_tickets.ticket_priority = zendesk_sla_policies.policy_metrics_priority)
