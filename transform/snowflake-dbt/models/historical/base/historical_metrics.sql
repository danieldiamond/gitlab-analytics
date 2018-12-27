WITH source AS (

	SELECT *
	FROM raw.historical.metrics

), renamed AS (


	SELECT uniquekey 										as primary_key,
			month::date 									as month_of,
			total_revenue::float 							as total_revenue,
			licensed_users::float 							as licensed_users,
			rev_per_user::float  							as revenue_per_user,
			com_paid_users::float 							as com_paid_users,
			active_core_hosts::float 						as active_core_hosts,
			com_availability::float 						as com_availability,
			com_response_time::float 						as com_response_time,
			com_monthly_active_users::float 				as com_monthly_active_users,
			com_projects::float 							as com_projects,
			ending_cash::float 								as ending_cash,
			ending_loc::float 								as ending_loc,
			cash_change::float 								as cash_change,
			avg_monthly_burn::float 						as avg_monthly_burn,
			days_outstanding::float							as days_outstanding,
			cash_remaining::float							as cash_remaining,
			rep_prod_annualized::float 						as rep_prod_annualized,
			cac::float 										as cac,
			ltv::float 										as ltv,
			ltv_to_cac::float								as ltv_to_cac,
			cac_ratio::float								as cac_ratio,
			magic_number::float								as magic_number,
			sales_efficiency::float							as sales_efficiency,
			gross_burn_rate::float 							as gross_burn_rate,
			cap_consumption::float 							as capital_consumption,
			sclau::float 									as sclau,
			csat::float 									as csat,
			on_prem_sla::float                				as on_prem_sla,
			sub_sla::float                    				as sub_sla

	FROM source

)

SELECT *
FROM renamed
