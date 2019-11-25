{{ config({
    "materialized": "table"
    })
}}

{{ schema_union_limit('snowplow', 'snowplow_sessions', 'session_start', 90) }}
