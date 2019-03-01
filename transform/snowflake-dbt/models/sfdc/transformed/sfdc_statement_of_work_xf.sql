{{ config(schema='analytics') }}

with sfdc_statement_of_work AS (

	SELECT * FROM {{ref('sfdc_statement_of_work')}}

), sfdc_users AS (

    SELECT * FROM {{ref('sfdc_users_xf')}}

)

SELECT sfdc_statement_of_work.*,
	    sfdc_users.name 		as sow_owner,
		sfdc_users.manager_name as sow_owner_manager,
		sfdc_users.department 	as sow_owner_department,
		sfdc_users.title 		as sow_owner_title
FROM sfdc_statement_of_work
LEFT JOIN sfdc_users
ON sfdc_users.id = sfdc_statement_of_work.owner_id