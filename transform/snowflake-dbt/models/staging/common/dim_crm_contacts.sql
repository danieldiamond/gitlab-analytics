WITH sfdc_leads AS (

    SELECT *
    FROM {{ ref('sfdc_lead_source') }}

), sfdc_contacts AS (

    SELECT *
    FROM {{ ref('sfdc_contact_source') }}

)

SELECT
  contact_id     AS sfdc_record_id,
  'contact'      AS sfdc_record_type,
  email_domain,
  account_id,
  person_score,
  contact_title  AS title
FROM sfdc_contacts
UNION
SELECT
  lead_id        AS sfdc_record_id,
  'lead'         AS sfdc_record_type,
  email_domain,
  NULL           AS account_id,
  person_score,
  title
FROM sfdc_leads
WHERE lead_email not in (
	SELECT DISTINCT contact_email from sfdc_contacts
  )
  AND is_converted = FALSE