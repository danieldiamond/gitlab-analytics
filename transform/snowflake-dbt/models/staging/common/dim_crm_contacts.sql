WITH sfdc_leads AS (

    SELECT *
    FROM {{ ref('sfdc_lead_source') }}

), sfdc_contacts AS (

    SELECT *
    FROM {{ ref('sfdc_contact_source') }}

)

SELECT
  --id
  contact_id           AS sfdc_record_id,
  'contact'            AS sfdc_record_type,
  email_domain,
  
  --keys
  master_record_id,
  owner_id,
  record_type_id,
  account_id,
  reports_to_id,

  --info
  person_score,
  contact_title        AS title,
  has_opted_out_email,
  email_bounced_date,
  email_bounced_reason,
  lead_source,
  lead_source_type

FROM sfdc_contacts

UNION

SELECT
  --id
  lead_id              AS sfdc_record_id,
  'lead'               AS sfdc_record_type,
  email_domain,
  
  --keys
  master_record_id,
  owner_id,
  record_type_id,
  NULL                 AS account_id,
  NULL                 AS reports_to_id,
  
  --info
  person_score,
  title,
  has_opted_out_email,
  email_bounced_date,
  email_bounced_reason,
  lead_source,
  lead_source_type

FROM sfdc_leads
WHERE is_converted = FALSE