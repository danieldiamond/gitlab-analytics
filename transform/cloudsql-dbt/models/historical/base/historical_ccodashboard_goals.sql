WITH source AS (

	SELECT md5(month_of :: varchar)                       as pk,
				 month_of :: date,
				 nullif(days_to_hire, '') :: float              as days_to_hire,
				 nullif(declined_candidate_score, '') :: float  as declined_candidate_score,
				 nullif("NPS_average", '') :: float             as nps_average,
				 nullif(candidates_per_vacancy, '') :: float    as candidates_per_vacancy,
				 nullif(low_rent_percentage, '') :: float       as low_rent_percentage,
				 nullif(average_rent_index, '') :: float        as average_rent_index,
				 nullif(vacancies_with_recruiting, '') :: float as vacancies_with_recruiting,
				 nullif(new_hire_avg_score, '') :: float        as new_hire_avg_score,
				 nullif(turnover_ratio, '') :: float            as turnover_ratio
	FROM historical.ccodashboard_goals
)

SELECT *
FROM source