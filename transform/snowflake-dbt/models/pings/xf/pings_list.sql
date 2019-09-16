{{ config({
    "schema": "staging"
    })
}}

with pings_usage_data AS (

	SELECT * FROM {{ ref('pings_usage_data') }}

)

SELECT distinct f.path as ping_name, 
		REPLACE(f.path, '.','__') as full_ping_name
FROM pings_usage_data,
lateral flatten(input => pings_usage_data.stats_used, recursive => True) f
WHERE IS_OBJECT(f.value) = FALSE
