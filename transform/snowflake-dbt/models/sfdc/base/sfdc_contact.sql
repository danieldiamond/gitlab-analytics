WITH source AS (

	SELECT *
	FROM {{ var("database") }}.salesforce_stitch.contact

), renamed AS(

	SELECT
		-- id
		id 							AS contact_id,
		name 						AS contact_name,
		email 						AS contact_email,

		-- keys
		accountid 					AS account_id,
		masterrecordid 				AS master_record_id,
		ownerid 					AS owner_id,
		recordtypeid 				AS record_type_id,
		reportstoid 				AS reports_to_id, 

		--contact info
		
		title 						AS contact_title,
		role__c 					AS contact_role,
		mobilephone 				AS mobile_phone,
		person_score__c 			AS person_score,

		department 					AS department,
		contact_status__c 			AS contact_status,
		requested_contact__c 		AS requested_contact,
		inactive_contact__c 		AS inactive_contact,
		hasoptedoutofemail 			AS has_opted_out_email,
		invalid_email_address__c 	AS invalid_email_address,
		isemailbounced 				AS email_is_bounced,
		emailbounceddate 			AS email_bounced_date,
		emailbouncedreason 			AS email_bounced_reason,

		--data_quality_score__c AS data_quality_score,
		--data_quality_description__c AS data_quality_description,

		mailingstreet 				AS mailing_address,
		mailingcity 				AS mailing_city,
		mailingstate 				AS mailing_state,
		mailingstatecode 			AS mailing_state_code,
		mailingcountry 				AS mailing_country,
		mailingcountrycode 			AS mailing_country_code,
		mailingpostalcode 			AS mailing_zip_code,

		-- info
		using_ce__c 				AS using_ce,
		ee_trial_start_date__c 		AS ee_trial_start_date,
		ee_trial_end_date__c 		AS ee_trial_end_date,
		industry__c 				AS industry,
		responded_to_githost_price_change__c AS responded_to_githost_price_change, -- maybe we can exclude this if it's not relevant
		leadsource 					AS lead_source,
		lead_source_type__c 		AS lead_source_type,
		outreach_stage__c 			AS outreach_stage,
		account_type__c 			AS account_type,
		mql_timestamp__c 			AS marketo_qualified_lead_timestamp,
		mql_datetime__c 			AS marketo_qualified_lead_datetime,
		mkto_si__last_interesting_moment__c 
									AS marketo_last_interesting_moment,
		mkto_si__last_interesting_moment_date__c 
									AS marketo_last_interesting_moment_date,


		--gl info
		account_owner__c 			AS account_owner,
		ae_comments__c 				AS ae_comments,
		business_development_rep__c AS business_development_rep_name,
		outbound_bdr__c 			AS outbound_business_development_rep_name,

		-- metadata
		createdbyid 				AS created_by_id,
		createddate 				AS created_date,
		lastactivitydate 			AS last_activity_date,
		lastcurequestdate 			AS last_cu_request_date,
		lastcuupdatedate 			AS last_cu_update_date,
		lastmodifiedbyid 			AS last_modified_by_id,
		lastmodifieddate 			AS last_modified_date,
		systemmodstamp


	FROM source
	WHERE isdeleted = FALSE

)

SELECT *
FROM renamed





------excluded

---- infer
--infer__infer_band__c AS ,
--infer__infer_hash__c AS ,
--infer__infer_last_modified__c AS ,
--infer__infer_rating__c AS ,
--infer__infer_score__c AS ,

---- lean data
--leandata__ld_segment__c AS ,
--leandata__matched_buyer_persona__c AS ,
--leandata__modified_score__c AS ,
--leandata__routing_action__c AS ,
--leandata__tag__c AS ,

----marketo
--mkto_si__add_to_marketo_campaign__c AS ,
--mkto_si__hidedate__c AS ,
--mkto_si__last_interesting_moment_desc__c AS ,
--mkto_si__last_interesting_moment_source__c AS ,
--mkto_si__last_interesting_moment_type__c AS ,
--mkto_si__mkto_lead_score__c AS ,
--mkto_si__priority__c AS ,
--mkto_si__relative_score__c AS ,
--mkto_si__relative_score_value__c AS ,
--mkto_si__sales_insight__c AS ,
--mkto_si__urgency__c AS ,
--mkto_si__urgency_value__c AS ,
--mkto_si__view_in_marketo__c AS ,
--mkto71_acquisition_date__c AS ,
--mkto71_acquisition_program__c AS ,
--mkto71_acquisition_program_id__c AS ,
--mkto71_inferred_city__c AS ,
--mkto71_inferred_company__c AS ,
--mkto71_inferred_country__c AS ,
--mkto71_inferred_metropolitan_area__c AS ,
--mkto71_inferred_phone_area_code__c AS ,
--mkto71_inferred_postal_code__c AS ,
--mkto71_inferred_state_region__c AS ,
--mkto71_lead_score__c AS ,
--mkto71_original_referrer__c AS ,
--mkto71_original_search_engine__c AS ,
--mkto71_original_search_phrase__c AS ,
--mkto71_original_source_info__c AS ,
--mkto71_original_source_type__c AS ,

----zendesk
--zendesk__create_in_zendesk__c AS ,
