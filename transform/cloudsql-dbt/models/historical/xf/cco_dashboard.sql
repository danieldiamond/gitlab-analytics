WITH goals AS (

	SELECT *,
			'goals'::varchar as metric_type
	FROM {{ref('historical_ccodashboard_goals')}}

), metrics AS (

	SELECT *,
			'performance'::varchar as metric_type
	FROM {{ref('historical_ccodashboard_goals')}}

), unioned AS (

	SELECT * FROM goals
	UNION ALL
	SELECT * FROM metrics

)

SELECT *
FROM unioned