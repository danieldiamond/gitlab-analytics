{{ config({
    "materialized": "ephemeral"
    })
}}

WITH gitlab_notes AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_internal_notes_xf')}}

), sfdc_accounts AS (

    SELECT *
    FROM {{ ref('sfdc_accounts_xf')}}

), sfdc_contacts AS (

    SELECT *
    FROM {{ ref('sfdc_contact_xf')}}

), sfdc_leads AS (

    SELECT *
    FROM {{ ref('sfdc_lead_xf')}}

), sfdc_opportunities AS (

    SELECT *
    FROM {{ ref('sfdc_opportunity_xf')}}

), zendesk_tickets AS (

    SELECT *
    FROM {{ ref('zendesk_tickets_xf')}}

), gitlab_notes_sfdc_id_flattened AS (

    SELECT
      gitlab_notes.note_id,
      noteable_id,
      noteable_type,
      {{target.schema}}_staging.id15to18(f.value::VARCHAR) AS sfdc_id_18char,
      SUBSTR(sfdc_id_18char, 0, 3)                         AS sfdc_id_prefix
    FROM gitlab_notes, 
      TABLE(flatten(sfdc_link_array)) f

), gitlab_notes_zendesk_ticket_id_flattened AS (

    SELECT
      gitlab_notes.note_id,
      noteable_id,
      noteable_type,
      f.value AS zendesk_ticket_id
    FROM gitlab_notes, 
      TABLE(flatten(zendesk_link_array)) f

), gitlab_notes_with_sfdc_objects AS (

    SELECT
      gitlab_notes_sfdc_id_flattened.noteable_id,
      gitlab_notes_sfdc_id_flattened.noteable_type,
      gitlab_notes_sfdc_id_flattened.sfdc_id_18char            AS link_id,
      CASE WHEN sfdc_id_prefix = '001' THEN 'Account'
           WHEN sfdc_id_prefix = '003' THEN 'Contact'
           WHEN sfdc_id_prefix = '00Q' THEN 'Lead'
           WHEN sfdc_id_prefix = '006' THEN 'Opportunity'
      ELSE NULL END                                            AS link_type,
      COALESCE(
          sfdc_accounts.account_id,
          sfdc_contacts.account_id,
          sfdc_leads.converted_account_id,
          sfdc_opportunities.account_id
      )                                                        AS sfdc_account_id
    FROM gitlab_notes_sfdc_id_flattened  
    LEFT JOIN sfdc_accounts 
      ON gitlab_notes_sfdc_id_flattened.sfdc_id_18char = sfdc_accounts.account_id
    LEFT JOIN sfdc_contacts
      ON gitlab_notes_sfdc_id_flattened.sfdc_id_18char = sfdc_contacts.contact_id
    LEFT JOIN sfdc_leads
      ON gitlab_notes_sfdc_id_flattened.sfdc_id_18char = sfdc_leads.lead_id
    LEFT JOIN sfdc_opportunities
      ON gitlab_notes_sfdc_id_flattened.sfdc_id_18char = sfdc_opportunities.opportunity_id
    WHERE link_type IS NOT NULL            

), gitlab_notes_with_zendesk_ticket AS (

    SELECT
      gitlab_notes_zendesk_ticket_id_flattened.noteable_id,
      gitlab_notes_zendesk_ticket_id_flattened.noteable_type,
      gitlab_notes_zendesk_ticket_id_flattened.zendesk_ticket_id AS link_id,
      'Zendesk Ticket'                                           AS link_type,
      zendesk_tickets.sfdc_account_id
    FROM gitlab_notes_zendesk_ticket_id_flattened
    INNER JOIN zendesk_tickets
      ON gitlab_notes_zendesk_ticket_id_flattened.zendesk_ticket_id::INTEGER = zendesk_tickets.ticket_id
    INNER JOIN sfdc_accounts
      ON zendesk_tickets.sfdc_account_id = sfdc_accounts.account_id

), gitlab_notes_with_sfdc_objects_union AS (

    SELECT *
    FROM gitlab_notes_with_sfdc_objects

    UNION

    SELECT *
    FROM gitlab_notes_with_zendesk_ticket

)

SELECT *
FROM gitlab_notes_with_sfdc_objects_union
