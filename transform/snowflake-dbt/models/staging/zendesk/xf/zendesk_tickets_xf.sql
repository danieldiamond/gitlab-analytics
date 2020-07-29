WITH zendesk_tickets AS (

    SELECT {{ dbt_utils.star(from=ref('zendesk_tickets_source'), except=['custom_fields']) }}
    FROM {{ref('zendesk_tickets_source')}}

), zendesk_users_source AS (

    SELECT *
    FROM {{ref('zendesk_users_source')}}

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
    FROM {{ref('zendesk_sla_policies_source')}}
    WHERE policy_metrics_metric = 'first_reply_time'

), zendesk_organizations AS (

    SELECT 
      organization_id,
      sfdc_account_id,
      organization_tags,
      organization_market_segment
    FROM {{ref('zendesk_organizations_source')}}

), zendesk_tickets_sla AS (

    SELECT *
    FROM {{ref('zendesk_tickets_sla_xf')}}

), zendesk_satisfaction_ratings AS (

    SELECT *
    FROM {{ref('zendesk_satisfaction_ratings_source')}}

), zendesk_ticket_audit_first_comment AS (

    SELECT
      ticket_id,
      audit_created_at                          AS sla_audit_created_at,
      IFF(audit_value = '0', FALSE, TRUE)       AS is_first_comment_public

    FROM {{ref('zendesk_ticket_audits')}}
    WHERE audit_field = 'is_public'
    QUALIFY ROW_NUMBER() OVER(PARTITION BY ticket_id ORDER BY sla_audit_created_at) = 1

)

SELECT DISTINCT 
  zendesk_tickets.*,
  zendesk_satisfaction_ratings.created_at                   AS satisfaction_rating_created_at,
  zendesk_ticket_metrics.first_reply_time,
  zendesk_organizations.sfdc_account_id,
  zendesk_organizations.organization_market_segment,
  zendesk_organizations.organization_tags,
  zendesk_tickets_sla.priority                              AS ticket_priority_at_first_reply,
  zendesk_tickets_sla.sla_policy                            AS ticket_sla_policy_at_first_reply,
  zendesk_tickets_sla.first_reply_time_sla,
  zendesk_tickets_sla.first_reply_at,
  zendesk_ticket_metrics.solved_at,
  zendesk_sla_policies.policy_metrics_target,
  IFF(zendesk_tickets_sla.first_reply_time_sla <= zendesk_sla_policies.policy_metrics_target, 
      True, False)                                          AS was_support_sla_met,
  IFF(zendesk_users_submitter.role = 'end-user'
    OR (
        zendesk_users_submitter.role IN ('agent', 'admin')
        AND zendesk_ticket_audit_first_comment.is_first_comment_public = FALSE
        ),
      TRUE, FALSE
    )                                                       AS is_part_of_sla,
  IFF(zendesk_tickets_sla.first_reply_time_sla <= zendesk_sla_policies.policy_metrics_target, 
      True, False)                                          AS was_sla_achieved,
  IFF(zendesk_tickets_sla.first_reply_time_sla > zendesk_sla_policies.policy_metrics_target,
      True, False)                                          AS was_sla_breached

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
LEFT JOIN zendesk_users_source AS zendesk_users_submitter
  ON zendesk_users_submitter.user_id = zendesk_tickets.submitter_id
LEFT JOIN zendesk_satisfaction_ratings
  ON zendesk_satisfaction_ratings.satisfaction_rating_id = zendesk_tickets.satisfaction_rating_id
LEFT JOIN zendesk_ticket_audit_first_comment
  ON zendesk_ticket_audit_first_comment.ticket_id = zendesk_tickets.ticket_id
