WITH source AS (

	SELECT *
	FROM historical.metrics

), renamed AS (


	SELECT uniquekey as primary_key,
			month::date as month_of,
			to_number(total_revenue, '99G999G999') as total_revenue,
			to_number(licensed_users, '99G999G999') as licensed_users,
			to_number(rev_per_user, '99G999G999') as revenue_per_user,
			to_number(com_paid_users,'99G999G999') as com_paid_users,
			to_number(active_core_hosts, '99G999G999') as active_core_hosts,
			com_availability,
			to_number(com_response_time,'99G999G999') as com_response_time,
			to_number(com_active_30_day_users,'99G999G999') as com_active_30_day_users,
			to_number(com_projects,'99G999G999') as com_projects,
			to_number(ending_cash,'99G999G999') as ending_cash,
			to_number(ending_loc,'99G999G999') as ending_loc,
			to_number(cash_change, '99G999G999') as cash_change,
			to_number(avg_monthly_burn, '99G999G999') as avg_monthly_burn,
			days_outstanding,
			cash_remaining,
			to_number(rep_prod_annualized, '99G999G999') as rep_prod_annualized,
			to_number(cac, '99G999G999') as cac, 
			to_number(ltv, '99G999G999') as ltv, 
			ltv_to_cac,
			cac_ratio,
			magic_number, 
			sales_efficiency, 
			to_number(gross_burn_rate, '99G999G999') as gross_burn_rate,
			to_number(cap_consumption, '99G999G999') as capital_consumption,
			
			--metadata
			TIMESTAMP 'epoch' + updated_at * INTERVAL '1 second' as updated_at
			
	FROM source

)

SELECT *
FROM renamed