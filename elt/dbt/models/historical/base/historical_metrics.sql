WITH source AS (

	SELECT *
	FROM historical.metrics

), renamed AS (


	SELECT uniquekey as primary_key,
			month::date as month_of,
			total_revenue, --recast to number
			licensed_users,
			rev_per_user as revenue_per_user,
			com_paid_users,
			active_core_hosts,
			com_availability,
			com_response_time,
			com_active_30_day_users,
			com_projects,
			ending_cash,
			ending_loc,
			cash_change,
			avg_monthly_burn,
			days_outstanding,
			cash_remaining,
			rep_prod_annualized,
			cac,
			ltv,
			ltv_to_cac,
			cac_ratio,
			magic_number,
			sales_efficiency,
			gross_burn_rate,
			cap_consumption,
			
			--metadata
			TIMESTAMP 'epoch' + updated_at * INTERVAL '1 second' as updated_at
			
	FROM source

)

SELECT *
FROM renamed