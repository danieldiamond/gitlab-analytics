WITH source AS (

	SELECT *
	FROM zendesk.tickets

),

    renamed AS(

	SELECT
		 id                              as ticket_id,
										-- keys
		 external_id,
		 requester_id,
		 submitter_id,
		 assignee_id,
		 organization_id,
		 group_id,
		 collaborator_ids,
		 follower_ids,
		 forum_topic_id,
		 problem_id,
		 sharing_agreement_ids,
		 followup_ids,
		 email_cc_ids,
		 via_followup_source_id,
		 macro_ids,
		 ticket_form_id,
		 brand_id,
										-- logistical info
		 type                            as ticket_type,
		 subject                         as ticket_subject,
		 description                     as ticket_description,
		 priority                        as ticket_priority,
		 status                          as ticket_status,
		 recipient,
		 has_incidents,
		 due_at,
		 tags,
		 via ->> 'channel'               as channel,
		 via ->> 'source'                as source,
		 custom_fields, -- need to understand use-case to denest
										--fields, --same as custom
		 satisfaction_rating ->> 'score' as satisfaction_rating,
		 allow_channelback               as does_allow_channelback,
		 is_public,
										-- metadata
		 url                             AS ticket_url,
		 created_at,
		 updated_at

    FROM source

)

SELECT *
FROM renamed