with actuals as (

	SELECT * FROM {{ref('sheetload_ccodashboard_actuals')}}

), goals as (

	SELECT * FROM {{ref('sheetload_ccodashboard_goals')}}

)

SELECT actuals.*,
		goals.days_to_hire as goal_days_to_hire,
		goals.declined_candidate_score as goal_declined_candidate_score,
		goals.nps_average as goal_nps_average,
		goals.candidates_per_vacancy as goal_candidates_per_vacancy,
		goals.low_rent_percentage as goal_low_rent_percentage,
		goals.average_rent_index as goal_average_rent_index,
		goals.vacancies_with_recruiting as goal_vacancies_with_recruiting,
		goals.new_hire_avg_score as goal_new_hire_avg_score,
		goals.turnover_ratio as goal_turnover_ratio  
FROM actuals
LEFT JOIN goals
ON actuals.pk = goals.pk