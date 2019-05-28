{#
-- Netsuite Docs: http://www.netsuite.com/help/helpcenter/en_US/srbrowser/Browser2016_1/schema/record/account.html
#}


with base as (
		SELECT *
		FROM {{ var("database") }}.netsuite_stitch.account

), renamed as (

		SELECT internalid as internal_account_id,
				--external_id as external_account_id,
			  	acctname as account_name,
			 	acctnumber as account_number,
			 	accttype as account_type,
			 	--billable_expenses_acct_id as billable_expenses_account_id,
			 	--billable_expenses_acct_name as billable_expenses_account_name,
			  --cash_flow_rate,
				--category1099misc_id,
				--category1099misc_name,
				--class_id,
				--class_name,
				--cur_doc_num,
				currency:internalId as currency_id,
			 	currency:name as currency_name,
			 	--deferral_acct_id,
				--deferral_acct_name,
				--department_id,
				--department_name,
				description as account_description,
				eliminate as is_intercompany_account,
				--exchange_rate,
				--general_rate, -- refers to exchange rate type
				includechildren as does_include_children,
				--inventory
				isinactive as is_inactive,
				--legal_name
				--location_id
				--location_name
				--opening_balance
				parent:internalId as parent_account_id,
				parent:name as parent_account_name,
				--restrict_to_accounting_book_list
				revalue,
				subsidiarylist -- is json --needs to be unnested
				--tran_date
				--unit_id
				--unit_name
				--units_type_id
				--units_type_name
				--custom_field_list
				--imported_at

		FROM base

)

SELECT *
FROM renamed
