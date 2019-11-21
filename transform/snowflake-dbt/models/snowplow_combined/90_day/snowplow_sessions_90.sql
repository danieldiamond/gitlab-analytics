{{ config({
    "materialized": "table"
    })
}}

{{ schema_union_limit('snowplow', 'snowplow_sessions', 90) }}
