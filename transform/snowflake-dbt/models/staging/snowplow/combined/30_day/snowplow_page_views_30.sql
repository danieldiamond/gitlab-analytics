{{ config({
    "materialized": "view"
    })
}}

{{ schema_union_limit('snowplow_', 'snowplow_page_views', 'page_view_start', 30) }}
