{{ config({
    "materialized": "table"
    })
}}

{{ schema_union_limit('snowplow', 'snowplow_unstructured', 'derived_tstamp', 90) }}
