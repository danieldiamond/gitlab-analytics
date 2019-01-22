with actuals as (

	SELECT * FROM {{ref('sheetload_crodashboard_actuals')}}

), goals as (

	SELECT * FROM {{ref('sheetload_crodashboard_goals')}}

)

SELECT actuals.*,
		goals.ramped_reps_on_quota as goal_ramped_reps_on_quota
FROM actuals
LEFT JOIN goals
ON actuals.pk = goals.pk

