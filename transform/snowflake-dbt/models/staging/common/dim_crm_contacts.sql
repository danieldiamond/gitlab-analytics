WITH sfdc_leads AS (

    SELECT *
    FROM {{ ref('sfdc_lead_source') }}

), sfdc_contacts AS (

    SELECT *
    FROM {{ ref('sfdc_contact_source') }}

)

SELECT
  sfdc_leads.lead_id,
  sfdc_contacts.contact_id 
from sfdc_leads
full outer join sfdc_contacts
  on sfdc_leads.converted_contact_id = sfdc_contacts.contact_id