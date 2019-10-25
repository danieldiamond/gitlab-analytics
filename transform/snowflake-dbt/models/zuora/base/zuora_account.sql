
{{config({
    "schema": "staging"
  })
}}

WITH source AS (

	SELECT *
    FROM {{ source('zuora', 'account') }}

), renamed AS(

	SELECT
		id                              					   AS account_id,
		-- keys
		communicationprofileid                                 AS communication_profile_id,
		nullif({{target.schema}}_staging.id15to18(crmid), '')  AS crm_id,
		defaultpaymentmethodid                                 AS default_payment_method_id,
		invoicetemplateid               					   AS invoice_template_id,
		parentid                                               AS parent_id,
		soldtocontactid                                        AS sold_to_contact_id,
		billtocontactid                                        AS bill_to_contact_id,
		taxexemptcertificateid                                 AS tax_exempt_certificate_id,
		taxexemptcertificatetype                               AS tax_exempt_certificate_type,

		-- account info
		accountnumber                                          AS account_number,
		name                                                   AS account_name,
		notes                                                  AS account_notes,
		purchaseordernumber                                    AS purchase_order_number,
		accountcode__c                                         AS sfdc_account_code,
		status,
		entity__c                                              AS sfdc_entity,

		autopay                                                AS auto_pay,
		balance                                                AS balance,
		creditbalance                                          AS credit_balance,
		billcycleday                                           AS bill_cycle_day,
		currency                                               AS currency,
		conversionrate__c                                      AS sfdc_conversion_rate,
		paymentterm                                            AS payment_term,

		allowinvoiceedit                                       AS allow_invoice_edit,
		batch,
		invoicedeliveryprefsemail                              AS invoice_delivery_prefs_email,
		invoicedeliveryprefsprint                              AS invoice_delivery_prefs_print,
		paymentgateway                                         AS payment_gateway,

		customerservicerepname                                 AS customer_service_rep_name,
		salesrepname                                           AS sales_rep_name,
		additionalemailaddresses                               AS additional_email_addresses,
		--billtocontact                   as bill_to_contact,
		parent__c                                              AS sfdc_parent,


		-- financial info
		lastinvoicedate                                        AS last_invoice_date,

		-- metadata
		createdbyid                                            AS created_by_id,
		createddate                                            AS created_date,
		updatedbyid                                            AS updated_by_id,
		updateddate                                            AS updated_date

	FROM source
	WHERE
		deleted = FALSE
		 AND
		id NOT IN
	-- Removes test accounts from Zuora
	    (
	        '2c92a008643512650164430b9c562527', -- WILSON GMBH TEST ACCOUNT
	        '2c92a0fc60202e4a0160503669826d14', -- Test Account
	        '2c92a0fd62b7fe7e0162d6e7993c2341', -- Test Estuate Account
	        '2c92a0ff5e09bd63015e0f4d01616d0d', -- Test Zuora Account
	        '2c92a0ff5e09bd69015e0f42f8c97cc9', -- Test Account Invoice Owner
	        '2c92a0fc5f33da20015f43ee78875ec2', -- Wilson Test
	        '2c92a0ff6446d76201644739829d1e33', -- Test DE
	        '2c92a0ff605102760160529eb44f287e', -- Wilson TEST
	        '2c92a0fd55767b97015579b5185d2a6e', -- Payment Gateway Testing
	        '2c92a0fe6477df2e0164888d62fc5628', -- Timostestcompany
	        '2c92a0fe55a0e4a50155a3a50d7b3de6', -- Wilson Lau
	        '2c92a0ff55a0e4910155a36b51e0389c' -- Wilson - Colorado
	    )


)

SELECT *
FROM renamed
WHERE account_id NOT IN (
  '2c92a00d6d8c712b016da9e999837b80' --https://gitlab.com/gitlab-data/analytics/issues/2689
  , '2c92a0076d8c63b3016dac8d5daf64f7' --https://gitlab.com/gitlab-data/analytics/issues/2689
  , '2c92a00c6d8c63ab016daca22e502ee6' --https://gitlab.com/gitlab-data/analytics/issues/2689
  , '2c92a0ff6d8c7188016dacb5b72d0f9e' --https://gitlab.com/gitlab-data/analytics/issues/2689
  , '2c92a0076db4eeaf016dcaa92c9810d2' --https://gitlab.com/gitlab-data/analytics/issues/2874
)
