WITH goals AS (

	SELECT *,
			'goals'::varchar as metric_type
	FROM {{ref('historical_ccodashboard_goals')}}

), metrics AS (

	SELECT 
			*,
			'actuals'::varchar as metric_type
	FROM {{ref('historical_ccodashboard_actuals')}}

), unioned AS (

	SELECT * FROM goals
	UNION ALL
	SELECT * FROM metrics

)

SELECT md5(month_of::varchar||metric_type) as primary_key,
		*
FROM unioned