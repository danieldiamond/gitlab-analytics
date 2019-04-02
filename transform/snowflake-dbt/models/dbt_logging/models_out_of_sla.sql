with models_out_of_sla as (

    SELECT model, 
            MAX(deployment_started_at) AS last_deployment_ts, 
            datediff('hour', MAX(deployment_started_at), getdate()) AS hours_since_refreshed
    FROM {{ ref('stg_dbt_model_deployments') }}
    GROUP BY 1
    HAVING MAX(deployment_started_at) < dateadd('hour', -24, getdate())

  )

SELECT count(1)
FROM models_out_of_sla