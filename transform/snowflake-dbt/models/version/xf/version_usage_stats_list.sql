{{ config({
    "schema": "staging"
    })
}}

WITH version_usage_data AS (

	SELECT * 
  FROM {{ ref('version_usage_data') }}

)

SELECT DISTINCT
  f.path                     AS stat_name, 
  REPLACE(f.path, '.','_')   AS full_stat_name
FROM version_usage_data,
  lateral flatten(input => pings_usage_data.stats_used, recursive => True) f
WHERE IS_OBJECT(f.value) = FALSE
