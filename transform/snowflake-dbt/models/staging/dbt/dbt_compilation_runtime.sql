WITH dbt_run_results AS (

    SELECT *
    FROM {{ ref('dbt_run_results_source') }}

), current_stats AS (

    SELECT 
      model_name, 
      TIMESTAMPDIFF('ms', compilation_started_at, compilation_completed_at) / 1000 AS compilation_time_seconds_elapsed
    FROM dbt_run_results
    WHERE compilation_started_at IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY model_name ORDER BY compilation_started_at DESC) = 1
    ORDER BY 2 desc

)

SELECT * 
FROM current_stats
