{{ config({
    "materialized": "table"
    })
}}

{{ schema_union_limit('snowplow', 'snowplow_page_views', 'page_view_start', 90) }}
