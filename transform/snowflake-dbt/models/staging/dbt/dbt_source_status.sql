WITH dbt_source AS (

    SELECT *
    FROM {{ ref('dbt_source') }}

), current_state AS (

    SELECT 
      table_name, 
      source_freshness_state, 
      freshness_observed_at,
      latest_load_at
    FROM dbt_source
    QUALIFY ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY freshness_observed_at DESC) = 1 
    ORDER by 2, 1
    
) 
SELECT * 
FROM current_state