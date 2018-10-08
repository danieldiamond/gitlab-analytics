WITH source AS (

	SELECT 
		  month_of::date,
		  days_to_hire,
		  declined_candidate_score,
		  "NPS_average" as nps_average,
		  candidates_per_vacancy,
		  low_rent_percentage,
		  average_rent_index,
		  vacancies_with_recruiting,
		  new_hire_avg_score,
		  turnover_ratio  
	FROM historical.ccodashboard_goals
)

SELECT *
FROM source