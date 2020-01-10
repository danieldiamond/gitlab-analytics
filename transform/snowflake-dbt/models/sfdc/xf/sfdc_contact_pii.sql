{{config({
    "schema": "sensitive"
  })
}}

WITH sfdc_contact AS (

    SELECT
		contact_id,
		contact_email,
		contact_name, 
		person_id
    FROM {{ ref('sfdc_contact') }}

), sfdc_contact_pii as (

	SELECT *
	FROM sfdc_contact

)

SELECT *
FROM sfdc_contact_pii