SELECT [deployment_started_at:aggregation] AS period, 
		model, 
		count(1) AS deployment_count
FROM  {{ ref('stg_dbt_model_deployments') }}
WHERE [deployment_started_at=daterange]
GROUP BY 1, 2
ORDER BY 1 DESC, 2