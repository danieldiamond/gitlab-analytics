{{ config(schema='analytics') }}

WITH source AS (

	SELECT *
	FROM {{ var("database") }}.sheetload.metrics

), renamed AS (


	SELECT uniquekey 										AS primary_key,
			month::date 									AS month_of,
			nullif(total_revenue::float, '')				AS total_revenue,
			nullif(licensed_users::float, '')				AS licensed_users,
			nullif(rev_per_user::float, '')					AS revenue_per_user,
			nullif(com_paid_users::float, '')				AS com_paid_users,
			nullif(active_core_hosts::float, '')			AS active_core_hosts,
			nullif(com_availability::float, '')				AS com_availability,
			nullif(com_response_time::float, '')			AS com_response_time,
			nullif(com_monthly_active_users::float, '')		AS com_monthly_active_users,
			nullif(com_projects::float, '')					AS com_projects,
			nullif(ending_cash::float, '')					AS ending_cash,
			nullif(ending_loc::float, '')					AS ending_loc,
			nullif(cash_change::float, '')					AS cash_change,
			nullif(avg_monthly_burn::float, '')				AS avg_monthly_burn,
			nullif(days_outstanding::float, '')				AS days_outstanding,
			nullif(cash_remaining::float, '')				AS cash_remaining,
			nullif(rep_prod_annualized::float, '')			AS rep_prod_annualized,
			nullif(cac::float, '')							AS cac,
			nullif(ltv::float, '')							AS ltv,
			nullif(ltv_to_cac::float, '')					AS ltv_to_cac,
			nullif(cac_ratio::float, '')					AS cac_ratio,
			nullif(magic_number::float, '')					AS magic_number,
			nullif(sales_efficiency::float, '')				AS sales_efficiency,
			nullif(gross_burn_rate::float, '')				AS gross_burn_rate,
			nullif(cap_consumption::float, '')				AS capital_consumption,
			nullif(sclau::float, '')						AS sclau,
			nullif(csat::float, '')							AS csat,
			nullif(on_prem_sla::float, '')					AS on_prem_sla,
			nullif(sub_sla::float, '')						AS sub_sla

	FROM source

)

SELECT *
FROM renamed
