WITH source AS (

	SELECT *
	FROM zuora.account

), renamed AS(

	SELECT 
		id as account_id,
		-- keys
		communicationprofileid as communication_profile_id,
		crmid as crm_id,
		defaultpaymentmethodid as default_payment_method_id,
		invoicetemplateid as invoice_template_id,
		parentid as parent_id,
		soldtocontactid as sold_to_contact_id,
		billtocontactid as bill_to_contact_id,
		taxexemptcertificateid as tax_exempt_certificate_id, 
		taxexemptcertificatetype as tax_exempt_certificate_type,

		-- account info
		accountnumber as account_number,
		name as account_name,
		notes as account_notes,
		purchaseordernumber as purchase_order_number,
		accountcode__c as sfdc_account_code,
		status, 
		entity__c as sfdc_entity,

		autopay as auto_pay,
		balance as balance,
		creditbalance as credit_balance,
		billcycleday as bill_cycle_day,
		currency as currency,
		conversionrate__c as sfdc_conversion_rate,
		paymentterm as payment_term,

		allowinvoiceedit as allow_invoice_edit,
		batch,
		invoicedeliveryprefsemail as invoice_delivery_prefs_email,
		invoicedeliveryprefsprint as invoice_delivery_prefs_print,
		paymentgateway as payment_gateway, 

		customerservicerepname as customer_service_rep_name,
		salesrepname as sales_rep_name,
		additionalemailaddresses as additional_email_addresses,
		billtocontact as bill_to_contact,
		parent__c as sfdc_parent,


		-- financial info
		lastinvoicedate as last_invoice_date,

		-- metadata
		createdbyid as created_by_id,
		createddate as created_date,
		updatedbyid as updated_by_id,
		updateddate as updated_date


	FROM source

)

SELECT *
FROM renamed