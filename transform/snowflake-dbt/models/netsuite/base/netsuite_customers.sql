{#
-- Netsuite Docs: http://www.netsuite.com/help/helpcenter/en_US/srbrowser/Browser2016_1/schema/record/customer.html
#}

with base as (

		SELECT *
		FROM raw.netsuite_stitch.netsuite_customer

), renamed as (

	SELECT internalid as customer_id,
		companyname as customer_name,
       	entityid as entity_name,
       	balance,
       	consolbalance as consolidated_balance,
       	consoloverduebalance as consolidated_balance_days_overdue,
       	overduebalance
    FROM base

)

SELECT *
FROM renamed





