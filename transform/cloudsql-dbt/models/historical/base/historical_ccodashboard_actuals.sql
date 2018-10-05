WITH source AS (

	SELECT  month_of::date as month_of,
			days_to_hire,
			"NPS_average",
			"Onboarding_eNPS",
			low_rent_percentage,
			average_rent_index,
			monthly_turnover_ratio,
			ytd_turnover_ratio,
			candidates_per_vacancy,
			declined_candidate_score,
			vacancies_with_recruiting
	FROM historical.ccodashboard_actuals

)

SELECT *
FROM source
