SELECT [deployment_started_at:aggregation] AS period, 
		sum(datediff('minute', deployment_started_at, deployment_completed_at)) AS total_runtime_m,
		avg(datediff('minute', deployment_started_at, deployment_completed_at)) AS avg_runtime_m, 
		sum(models_deployed) AS models_deployed
FROM {{ ref('stg_dbt_deployments') }}
WHERE [deployment_started_at=daterange]
GROUP BY 1
ORDER BY 1 DESC