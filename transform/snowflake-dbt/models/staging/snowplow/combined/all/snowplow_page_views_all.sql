{{config({
    "materialized":"view"
  })
}}

{{ schema_union_all('snowplow_', 'snowplow_page_views') }}
