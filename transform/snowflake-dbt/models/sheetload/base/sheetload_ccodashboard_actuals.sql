WITH source AS (

	SELECT md5(month_of :: varchar)                       as pk,
				 month_of :: date 								as month_of,
				 nullif(days_to_hire, '') :: float              as days_to_hire,
				 nullif("NPS_average", '') :: float             as nps_average,
				 nullif("Onboarding_eNPS", '') :: float         as onboarding_enps,
				 nullif(low_rent_percentage, '') :: float       as low_rent_percentage,
				 nullif(average_rent_index, '') :: float        as average_rent_index,
				 nullif(monthly_turnover_ratio, '') :: float    as monthly_turnover_ratio,
				 nullif(ytd_turnover_ratio, '') :: float        as ytd_turnover_ratio,
				 nullif(candidates_per_vacancy, '') :: float    as candidates_per_vacancy,
				 nullif(declined_candidate_score, '') :: float  as declined_candidate_score,
				 nullif(vacancies_with_recruiting, '') :: float as vacancies_with_recruiting
	FROM {{ var("database") }}.sheetload.ccodashboard_actuals
)

SELECT *
FROM source
