{{ config({
    "schema": "analytics",
    "post-hook": "grant select on {{this}} to role reporter"
    })
}}

WITH zendesk_tickets_xf AS (

  SELECT *
  FROM {{ref('zendesk_tickets_xf')}}


), sfdc_accounts AS (

  SELECT account_id                             AS dupe_account_id, 
          account_name, 
          gitlab_entity, 
          account_owner, 
          account_owner_team, 
          account_type, 
          customer_since_date,
          next_renewal_date, 
          account_region, 
          total_account_value, 
          account_sub_region, 
          support_level, 
          count_ce_instances, 
          count_active_ce_users, 
          count_using_ce
  FROM {{ref('sfdc_accounts_xf')}}

), sfdc_opportunity AS (

  SELECT *
  FROM {{ref('sfdc_opportunity_xf')}}
)

SELECT * 
FROM zendesk_tickets_xf
INNER JOIN sfdc_accounts 
    ON zendesk_tickets_xf.sfdc_id = sfdc_accounts.dupe_account_id
INNER JOIN sfdc_opportunity
    ON zendesk_tickets_xf.sfdc_id = sfdc_opportunity.account_id 
