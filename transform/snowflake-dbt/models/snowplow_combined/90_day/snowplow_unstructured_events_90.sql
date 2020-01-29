{{ config({
    "materialized": "table"
    })
}}

{{ schema_union_limit('snowplow', 'snowplow_unstructured_events', 'derived_tstamp', 90) }}
