
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
	        '2c92a0ff55a0e4910155a36b51e0389c', -- Wilson - Colorado
          '2c92a00769fba8700169fd92caf649b3',
          '2c92a0ff65a88c5b0165aac56f5c7418', -- from here https://gitlab.com/gitlab-data/analytics/issues/3041
          '2c92a0ff60203d2c0160230547c46bed',
          '2c92a0ff69767b5b01698dafe0301fac',
          '2c92a00e6b54c11a016b55dd955049ad',
          '2c92a0076d713cf9016d844b5d713a8b',
          '2c92a0ff6a07f4e8016a0d36eb24677a',
          '2c92a0fc6e17459e016e17f8f8157b06',
          '2c92a00c6c297dc9016c2b0ea3f36836',
          '2c92a0086d4dcd5a016d5e8fe90f72a7',
          '2c92a00e6a48a2af016a49fb66654f72',
          '2c92a0fe6dd9896c016ddb54e5a55492',
          '2c92a0fe688ada4201689112131e0636',
          '2c92a0076cb918a1016cba09f60a7a23',
          '2c92a0fc68a2b3820168a4c16bdb0a7f',
          '2c92a0fc5e7eed8a015e7fec1c3939be',
          '2c92a0ff627f5b06016287729e414b69',
          '2c92a00d6db4ffeb016dce0b2d506aa0',
          '2c92a0ff68bf6b420168c8b7a8134a5c',
          '2c92a0fe610e7378016128c62f401b48',
          '2c92a0fe65a87f0f0165aa4ae9bf3278',
          '2c92a00e6bc8f90a016bd8b03d4e76c0',
          '2c92a0086d714ace016d8264a7882709',
          '2c92a00d6d4dcd56016d5bd75b467b93' -- to here: https://gitlab.com/gitlab-data/analytics/issues/3041
	    )


)

SELECT *
FROM renamed
