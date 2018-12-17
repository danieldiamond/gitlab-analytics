with sfdc_executive_business_review AS (

	SELECT * FROM {{ref('sfdc_executive_business_review')}}

), sfdc_users AS (

    SELECT * FROM {{ref('users')}}

)

SELECT sfdc_executive_business_review.*,
	    sfdc_users.name 		as ebr_owner,
		sfdc_users.manager_name as ebr_owner_manager,
		sfdc_users.department 	as ebr_owner_department,
		sfdc_users.title 		as ebr_owner_title
FROM sfdc_executive_business_review
LEFT JOIN sfdc_users
ON sfdc_users.id = sfdc_executive_business_review.owner_id