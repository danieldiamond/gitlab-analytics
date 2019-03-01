{{ config(schema='analytics') }}

WITH source as (

    SELECT *
    FROM {{ var("database") }}.gcloud_postgres_stitch.version_usage_data
)

SELECT *, parse_json(stats) as stats_used
FROM source
WHERE uuid IS NOT NULL