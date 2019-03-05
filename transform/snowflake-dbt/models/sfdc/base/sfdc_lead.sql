WITH source AS (

	SELECT *
	FROM {{ var("database") }}.salesforce_stitch.lead

), renamed AS(

	SELECT
		--id
		id 							AS lead_id,
		name 						AS lead_name,
		email 						AS lead_email,

		--keys
		masterrecordid 				AS master_record_id,
		convertedaccountid 			AS converted_account_id,
		convertedcontactid 			AS converted_contact_id,
		convertedopportunityid 		AS converted_opportunity_id,
		ownerid 					AS owner_id,
		recordtypeid 				AS record_type_id,
		round_robin_id__c 			AS round_robin_id,
		instance_uuid__c 			AS instance_uuid,


		--lead info
		isconverted 				AS is_converted,
		converteddate 				AS converted_date,
		title 						AS title,
		donotcall 					AS is_do_not_call,
		hasoptedoutofemail 			AS has_opted_out_email,
		emailbounceddate 			AS email_bounced_date,
		emailbouncedreason 			AS email_bounced_reason,

		leadsource 					AS lead_source,
		lead_from__c 				AS lead_from,
		lead_source_type__c 		AS lead_source_type,
		lead_conversion_approval_status__c 
									AS lead_conversiona_approval_status,

		street 						AS street,
		city 						AS city,
		state 						AS state,
		statecode 					AS state_code,
		country 					AS country,
		countrycode 				AS country_code,
		postalcode 					AS postal_code,

		-- info
		requested_contact__c 		AS requested_contact,
		company 					AS company,
		buying_process_for_procuring_gitlab__c 
									AS buying_process,
		industry 					AS industry,
		region__c 					AS region,
		largeaccount__c 			AS is_large_account,
		outreach_stage__c 			AS outreach_stage,
		--data_quality_score__c AS data_quality_score,
		--data_quality_description__c AS data_quality_score_description,
		interested_in_gitlab_ee__c 	AS is_interested_gitlab_ee,
		interested_in_hosted_solution__c 
									AS is_interested_in_hosted,

		matched_account_top_list__c AS matched_account_top_list,
		mql_date__c 				AS marketo_qualified_lead_date,
		mql_datetime__c 			AS marketo_qualified_lead_datetime,

		--gitlab internal

		bdr_lu__c 					AS business_development_look_up,
		business_development_rep_contact__c 
									AS business_development_representative_contact,
		business_development_representative__c 
									AS business_development_representative,
		competition__c 				AS competition,


		--metadata
		createdbyid 				AS created_by_id,
		createddate 				AS created_date,
		lastactivitydate 			AS last_activity_date,
		lastmodifiedbyid 			AS last_modified_id,
		lastmodifieddate 			AS last_modified_date,
		systemmodstamp


	FROM source
	WHERE isdeleted = FALSE

)

SELECT *
FROM renamed



------- external
--infer

--infer__infer_band__c
--infer__infer_converted_to_opp__c
--infer__infer_created_month__c
--infer__infer_good_lead__c
--infer__infer_hash__c
--infer__infer_is_open__c
--infer__infer_last_modified__c
--infer__infer_lead_age__c
--infer__infer_rating__c
--infer__infer_score__c
--infer_web_direct_fit_score__c
--infer_web_direct_hash__c
--infer_web_direct_last_modified__c
--
--
------lean data
--leandata__a2b_account__c
--leandata__a2b_group__c
--leandata__has_matched__c
--leandata__ld_segment__c
--leandata__marketing_sys_created_date__c
--leandata__matched_account__c
--leandata__matched_account_annual_revenue__c
--leandata__matched_account_billing_country__c
--leandata__matched_account_billing_postal_code__c
--leandata__matched_account_billing_state__c
--leandata__matched_account_custom_field_1__c
--leandata__matched_account_employees__c
--leandata__matched_account_industry__c
--leandata__matched_account_name__c
--leandata__matched_account_type__c
--leandata__matched_account_website__c
--leandata__matched_buyer_persona__c
--leandata__matched_lead__c
--leandata__modified_score__c
--leandata__reporting_matched_account__c
--leandata__reporting_timestamp__c
--leandata__router_status__c
--leandata__routing_action__c
--leandata__routing_status__c
--leandata__salesforce_id__c
--leandata__search__c
--leandata__search_index__c
--leandata__status_info__c
--leandata__tag__c
--
------marketo
--mkto_si__add_to_marketo_campaign__c
--mkto_si__hidedate__c
--mkto_si__last_interesting_moment__c
--mkto_si__last_interesting_moment_date__c
--mkto_si__last_interesting_moment_desc__c
--mkto_si__last_interesting_moment_source__c
--mkto_si__last_interesting_moment_type__c
--mkto_si__msicontactid__c
--mkto_si__priority__c
--mkto_si__relative_score__c
--mkto_si__relative_score_value__c
--mkto_si__urgency__c
--mkto_si__urgency_value__c
--mkto_si__view_in_marketo__c
--mkto2__inferred_city__c
--mkto2__inferred_company__c
--mkto2__inferred_country__c
--mkto2__inferred_state_region__c
--mkto2__lead_score__c
--mkto71_acquisition_date__c
--mkto71_acquisition_program__c
--mkto71_acquisition_program_id__c
--mkto71_inferred_city__c
--mkto71_inferred_company__c
--mkto71_inferred_country__c
--mkto71_inferred_metropolitan_area__c
--mkto71_inferred_phone_area_code__c
--mkto71_inferred_postal_code__c
--mkto71_inferred_state_region__c
--mkto71_lead_score__c
--mkto71_original_referrer__c
--mkto71_original_search_engine__c
--mkto71_original_search_phrase__c
--mkto71_original_source_info__c
--mkto71_original_source_type__c
--
------zendesk
--zendesk__create_in_zendesk__c--
