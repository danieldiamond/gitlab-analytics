{{config({
    "materialized":"view"
  })
}}

{{ schema_union_all('snowplow_', 'snowplow_web_events') }}
