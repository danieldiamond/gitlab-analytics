WITH source_status AS (
    
    SELECT *
    FROM {{ ref('dbt_source_status') }}

), filtered_to_snapshots AS (

    select DISTINCT
      table_name, 
      date_trunc('d', latest_load_at) AS successful_load_at 
    FROM dbt_source
    WHERE table_name like '%snapshot%'
    ORDER BY 2 DESC

)

SELECT *
FROM filtered_to_snapshots

