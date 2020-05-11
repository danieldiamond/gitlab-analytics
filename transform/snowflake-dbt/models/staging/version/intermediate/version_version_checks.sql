{{ config({
    "materialized": "table"
    })
}}

SELECT *
FROM {{ ref('version_version_checks_source') }}