-- This file is loaded through dbt seed, your local runs will break unless you run dbt seed first.

WITH source as (

    SELECT * FROM {{ ref('ping_metrics_to_stage_mapping_data') }}
)

SELECT * FROM source