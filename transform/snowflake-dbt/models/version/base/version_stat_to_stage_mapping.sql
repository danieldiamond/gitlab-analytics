-- This file is loaded through dbt seed, your local runs will break unless you run dbt seed first.
{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ ref('version_metrics_to_stage_mapping_data') }}
)

SELECT * FROM source