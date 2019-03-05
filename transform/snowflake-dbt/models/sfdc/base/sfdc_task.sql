WITH source AS (

	SELECT *
	FROM {{ var("database") }}.salesforce_stitch.task

), renamed AS(

	SELECT
	id             AS task_id,
		
		--keys
	accountid      AS account_id,
	ownerid        AS owner_id,
	whoid          AS lead_or_contact_id,
		
		--info		
	subject        AS task_subject,
        activitydate   AS task_date
		
		--data_quality_description__c as data_quality_description,
		--data_quality_score__c as data_quality_score,
		--projections
		--results
		--metadata

	FROM source
	WHERE isdeleted = FALSE

)

SELECT *
FROM renamed


--- excluded columns
