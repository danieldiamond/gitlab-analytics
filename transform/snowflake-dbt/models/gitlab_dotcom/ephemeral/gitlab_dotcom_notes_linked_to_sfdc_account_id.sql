{{ config({
    "materialized": "ephemeral"
    })
}}

WITH gitlab_notes AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_internal_notes_xf')}}

)

, sfdc_accounts AS (

  SELECT *
  FROM {{ ref('sfdc_accounts_xf')}}

)

, sfdc_contacts AS (

  SELECT *
  FROM {{ ref('sfdc_contact_xf')}}

)

, sfdc_leads AS (

  SELECT *
  FROM {{ ref('sfdc_lead_xf')}}

)

, sfdc_opportunities AS (

  SELECT *
  FROM {{ ref('sfdc_opportunity_xf')}}

)

, zendesk_tickets AS (

  SELECT *
  FROM analytics.zendesk_tickets_xf

)

, gitlab_notes_sfdc_id_flattened AS (

  SELECT
    gitlab_notes.note_id,
    noteable_id,
    noteable_type,
    {{target.schema}}_staging.id15to18(CAST(f.value AS VARCHAR)) AS sfdc_id_18char

  FROM gitlab_notes, table(flatten(sfdc_link_array)) f
)

, gitlab_notes_zendesk_ticket_id_flattened AS (

  SELECT
    gitlab_notes.note_id,
    noteable_id,
    noteable_type,
    CAST(f.value AS INTEGER) AS zendesk_ticket_id

  FROM gitlab_notes, table(flatten(zendesk_link_array)) f
)

, gitlab_notes_with_sfdc_accounts AS (

  SELECT
    gitlab_notes_sfdc_id_flattened.note_id,
    gitlab_notes_sfdc_id_flattened.noteable_id,
    gitlab_notes_sfdc_id_flattened.noteable_type,
    sfdc_accounts.account_id AS sfdc_account_id

  FROM gitlab_notes_sfdc_id_flattened
  INNER JOIN sfdc_accounts
    ON gitlab_notes_sfdc_id_flattened.sfdc_id_18char = sfdc_accounts.account_id

)

, gitlab_notes_with_sfdc_opportunities AS (

  SELECT
    gitlab_notes_sfdc_id_flattened.note_id,
    gitlab_notes_sfdc_id_flattened.noteable_id,
    gitlab_notes_sfdc_id_flattened.noteable_type,
    sfdc_opportunities.account_id AS sfdc_account_id

  FROM gitlab_notes_sfdc_id_flattened
  INNER JOIN sfdc_opportunities
    ON gitlab_notes_sfdc_id_flattened.sfdc_id_18char = sfdc_opportunities.opportunity_id

)

, gitlab_notes_with_sfdc_leads AS (

  SELECT
    gitlab_notes_sfdc_id_flattened.note_id,
    gitlab_notes_sfdc_id_flattened.noteable_id,
    gitlab_notes_sfdc_id_flattened.noteable_type,
    sfdc_leads.converted_account_id AS sfdc_account_id

  FROM gitlab_notes_sfdc_id_flattened
  INNER JOIN sfdc_leads
    ON gitlab_notes_sfdc_id_flattened.sfdc_id_18char = sfdc_leads.lead_id

)

, gitlab_notes_with_sfdc_contacts AS (

  SELECT
    gitlab_notes_sfdc_id_flattened.note_id,
    gitlab_notes_sfdc_id_flattened.noteable_id,
    gitlab_notes_sfdc_id_flattened.noteable_type,
    sfdc_contacts.account_id AS sfdc_account_id

  FROM gitlab_notes_sfdc_id_flattened
  INNER JOIN sfdc_contacts
    ON gitlab_notes_sfdc_id_flattened.sfdc_id_18char = sfdc_contacts.contact_id
)

, gitlab_notes_with_zendesk_ticket AS (

  SELECT
    gitlab_notes_zendesk_ticket_id_flattened.note_id,
    gitlab_notes_zendesk_ticket_id_flattened.noteable_id,
    gitlab_notes_zendesk_ticket_id_flattened.noteable_type,
    zendesk_tickets.sfdc_account_id

  FROM gitlab_notes_zendesk_ticket_id_flattened
  INNER JOIN zendesk_tickets
    ON gitlab_notes_zendesk_ticket_id_flattened.zendesk_ticket_id = zendesk_tickets.ticket_id
  INNER JOIN sfdc_accounts
    ON zendesk_tickets.sfdc_account_id = sfdc_accounts.account_id
)

, gitlab_notes_with_sfdc_objects_union AS (

  SELECT
    *
  FROM gitlab_notes_with_sfdc_accounts

  UNION

  SELECT
    *
  FROM gitlab_notes_with_sfdc_opportunities

  UNION

  SELECT
    *
  FROM gitlab_notes_with_sfdc_contacts
  
  UNION

  SELECT
    *
  FROM gitlab_notes_with_zendesk_ticket

)

SELECT *
FROM gitlab_notes_with_sfdc_objects_union
