{{ config({
    "materialized": "table"
    })
}}

{{ schema_union_limit('snowplow', 'snowplow_gitlab_events', 'derived_tstamp', 30) }}
