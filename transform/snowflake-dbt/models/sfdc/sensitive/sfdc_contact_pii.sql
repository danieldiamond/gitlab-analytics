WITH source AS (

    SELECT *
    FROM {{ source('salesforce', 'contact') }}

), sfdc_contact_pii AS (

    SELECT
		id 				AS contact_id,
		sha1(email) 	AS person_id,
		email 			AS contact_email,
		name 			AS contact_name
    FROM source

)

SELECT *
FROM sfdc_contact_pii