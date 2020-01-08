{{config({
    "schema": "sensitive"
  })
}}

WITH sfdc_lead AS (

    SELECT
		lead_id,
		lead_email,
		lead_name
    FROM {{ ref('sfdc_lead') }}

), sfdc_lead_pii as (

	SELECT *
	FROM sfdc_lead

)

SELECT *
FROM sfdc_lead_pii