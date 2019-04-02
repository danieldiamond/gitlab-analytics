SELECT  model, 
		cast(min(deployment_started_at) AS date) AS first, 
		max(deployment_started_at) AS last
FROM {{ ref('stg_dbt_model_deployments') }}
WHERE [deployment_started_at=daterange]
GROUP BY 1
ORDER BY 3 ASC