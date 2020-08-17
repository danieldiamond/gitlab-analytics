WITH sfdc_leads AS(

  SELECT *
  FROM {{ ref('sfdc_lead_source') }}

), event_leadconversion AS(

  SELECT
    
	lead_history.lead_history_id        AS event_id,
	lead_history.field_modified_at      AS event_timestamp,
    lead_history.lead_id                AS lead_id,
	lead_history.created_by_id          AS crm_user_id,
	sfdc_leads.converted_contact_id     AS contact_id,
	sfdc_leads.converted_account_id     AS account_id,
	sfdc_leads.converted_opportunity_id AS opportunity_id,
	'lead conversion'                   AS event_name

  FROM {{ ref('sfdc_lead_history_source') }} lead_history
  INNER JOIN sfdc_leads
    ON sfdc_leads.lead_id = lead_history.lead_id

)

SELECT *
FROM event_leadconversion