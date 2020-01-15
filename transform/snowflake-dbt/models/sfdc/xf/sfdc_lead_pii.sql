{{config({
    "schema": "sensitive"
  })
}}

WITH sfdc_lead_pii AS (

    SELECT
		lead_id,
		lead_email,
		lead_name, 
		person_id
    FROM {{ ref('sfdc_lead') }}

)

SELECT *
FROM sfdc_lead_pii