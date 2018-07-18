WITH source AS (

	SELECT *
	FROM historical.metrics

), renamed AS (


	SELECT uniquekey as primary_key,
			month::date as month_of,
			regexp_replace(total_revenue, '[^a-zA-Z0-9]+', '','g')::float as total_revenue,
			regexp_replace(licensed_users, '[^a-zA-Z0-9]+', '','g')::float as licensed_users,
			(regexp_replace(rev_per_user, '[^a-zA-Z0-9]+', '','g')::float)/100  as revenue_per_user,
			regexp_replace(com_paid_users,'[^a-zA-Z0-9]+', '','g')::float as com_paid_users,
			regexp_replace(active_core_hosts, '[^a-zA-Z0-9]+', '','g')::float as active_core_hosts,
			(regexp_replace(com_availability,'[^a-zA-Z0-9]+', '','g')::float/100) as com_availability,
			regexp_replace(com_response_time,'[^a-zA-Z0-9]+', '','g')::float as com_response_time,
			regexp_replace(com_active_30_day_users,'[^a-zA-Z0-9]+', '','g')::float as com_active_30_day_users,
			regexp_replace(com_projects,'[^a-zA-Z0-9]+', '','g')::float as com_projects,
			regexp_replace(ending_cash,'[^a-zA-Z0-9]+', '','g')::float as ending_cash,
			regexp_replace(ending_loc,'[^a-zA-Z0-9]+', '','g')::float as ending_loc,
			regexp_replace(cash_change, '[^a-zA-Z0-9]+', '','g')::float as cash_change,
			regexp_replace(avg_monthly_burn, '[^a-zA-Z0-9]+', '','g')::float as avg_monthly_burn,
			days_outstanding,
			cash_remaining,
			regexp_replace(rep_prod_annualized, '[^a-zA-Z0-9]+', '','g')::float as rep_prod_annualized,
			regexp_replace(cac, '[^a-zA-Z0-9]+', '','g')::float as cac, 
			regexp_replace(ltv, '[^a-zA-Z0-9]+', '','g')::float as ltv, 
			ltv_to_cac,
			cac_ratio,
			magic_number, 
			sales_efficiency, 
			regexp_replace(gross_burn_rate, '[^a-zA-Z0-9]+', '','g')::float as gross_burn_rate,
			regexp_replace(cap_consumption, '[^a-zA-Z0-9]+', '','g')::float as capital_consumption,
			
			--metadata
			TIMESTAMP 'epoch' + updated_at * INTERVAL '1 second' as updated_at
			
	FROM source

)

SELECT *
FROM renamed