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

, gitlab_notes_sfdc_id_flattened AS (

  SELECT
    gitlab_notes.note_id,
    noteable_id,
    noteable_type,
    {{target.schema}}_staging.id15to18(CAST(f.value AS VARCHAR)) AS "18_sfdc_id"

  FROM gitlab_notes, table(flatten(sfdc_link_array)) f
)

, gitlab_notes_with_sfdc_accounts AS (

  SELECT
    gitlab_notes_sfdc_id_flattened.note_id,
    gitlab_notes_sfdc_id_flattened.noteable_id,
    gitlab_notes_sfdc_id_flattened.noteable_type,
    sfdc_accounts.account_id AS sfdc_account_id

  FROM gitlab_notes_sfdc_id_flattened
  INNER JOIN sfdc_accounts
    ON gitlab_notes_sfdc_id_flattened."18_sfdc_id" = sfdc_accounts.account_id

)
, gitlab_notes_with_sfdc_opportunities AS (

  SELECT
    gitlab_notes_sfdc_id_flattened.note_id,
    gitlab_notes_sfdc_id_flattened.noteable_id,
    gitlab_notes_sfdc_id_flattened.noteable_type,
    sfdc_opportunities.account_id AS sfdc_account_id

  FROM gitlab_notes_sfdc_id_flattened
  INNER JOIN sfdc_opportunities
    ON gitlab_notes_sfdc_id_flattened."18_sfdc_id" = sfdc_opportunities.opportunity_id

)
, gitlab_notes_with_sfdc_leads AS (

  SELECT
    gitlab_notes_sfdc_id_flattened.note_id,
    gitlab_notes_sfdc_id_flattened.noteable_id,
    gitlab_notes_sfdc_id_flattened.noteable_type,
    sfdc_leads.converted_account_id AS sfdc_account_id

  FROM gitlab_notes_sfdc_id_flattened
  INNER JOIN sfdc_leads
    ON gitlab_notes_sfdc_id_flattened."18_sfdc_id" = sfdc_leads.lead_id

)
, gitlab_notes_with_sfdc_contacts AS (

  SELECT
    gitlab_notes_sfdc_id_flattened.note_id,
    gitlab_notes_sfdc_id_flattened.noteable_id,
    gitlab_notes_sfdc_id_flattened.noteable_type,
    sfdc_contacts.account_id AS sfdc_account_id

  FROM gitlab_notes_sfdc_id_flattened
  INNER JOIN {{ ref('sfdc_contact_xf')}} AS sfdc_contacts
    ON gitlab_notes_sfdc_id_flattened."18_sfdc_id" = sfdc_contacts.contact_id
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

)

SELECT *
FROM gitlab_notes_with_sfdc_objects_union
