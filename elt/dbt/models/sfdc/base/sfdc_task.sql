WITH source AS (

	SELECT *
	FROM sfdc.task

), renamed AS(

	SELECT
		-- id
		id as task_id,

		--keys
		accountid as account_id,
		ownerid as owner_id,
		whatid as what_id,
		whoid as who_id,
		recordtypeid as record_type_id,
		pf_order_id__c as pf_order_id,

		--info
		priority as priority,
		status as status,
		subject as subject,
		type as task_type,
		tasksubtype as tasks_subtype,
		mass_email_activities__c as mass_email_activities,
		activity_source__c as activity,
		activity_disposition__c as activity_details,
		activitydate as activity_date,
		whatcount as what_count,
		whocount as who_count,
		account_owner_manager_email__c as account_owner_manager_email,
		description as description,
		event_disposition__c as event_disposition,
		calldisposition as call_disposition,
		calldurationinseconds as call_duration_in_seconds,
		callobject as call_object,
		calltype as call_type,
		close_task__c as close_task,

		is_answered__c as is_answered,
		is_bad_number__c as is_bad_number,
		is_busy__c as is_busy,
		is_correct_contact__c as is_correct_contact,
		is_left_message__c as is_left_message,
		is_not_answered__c as is_not_answered,
		isarchived as is_archived,
		isclosed as is_closed,
		ishighpriority as is_high_priority,
		isrecurrence as is_recurrence,
		isreminderset as is_reminder_set,
		reminderdatetime as reminder_date_time,

		data_quality_score__c as data_quality_score,
		data_quality_description__c as data_quality_score_description,

		--metadata
		createdbyid as created_by_id,
		createddate as created_date,
		lastmodifiedbyid as last_modified_by_id,
		lastmodifieddate as last_modified_date,
		systemmodstamp



	FROM source
	WHERE isdeleted IS FALSE

)

SELECT *
FROM renamed