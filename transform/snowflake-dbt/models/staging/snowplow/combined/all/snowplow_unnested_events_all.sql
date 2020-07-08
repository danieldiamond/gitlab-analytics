{{config({
    "materialized":"view"
  })
}}

{{ schema_union_all('snowplow', 'snowplow_unnested_events') }}
