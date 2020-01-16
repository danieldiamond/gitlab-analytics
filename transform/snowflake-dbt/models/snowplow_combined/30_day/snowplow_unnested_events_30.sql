{{ config({
    "materialized": "table"
    })
}}

{{ schema_union_limit('snowplow', 'snowplow_unnested_events', 'derived_tstamp', 30) }}
