WITH sfdc_contact_pii AS (

    SELECT
		contact_id,
		contact_email,
		contact_name, 
		person_id
    FROM {{ ref('sfdc_contact') }}

)

SELECT *
FROM sfdc_contact_pii