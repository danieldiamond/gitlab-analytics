{% set year_value = var('year', run_started_at.strftime('%Y')) %}
{% set month_value = var('month', run_started_at.strftime('%m')) %}

{{config({
    "materialized":"table",
    "unique_key":"base64_event",
    "schema":"snowplow_" + year_value|string + '_' + month_value|string, 
  })
}}

WITH gitlab as (

    SELECT *
    FROM {{ ref('snowplow_gitlab_bad_events') }}

),

fishtown as (

    SELECT *
    FROM {{ ref('snowplow_fishtown_bad_events') }}

),

unioned AS (

    SELECT *
    FROM gitlab

    UNION ALL

    SELECT *
    FROM fishtown

)

SELECT *
FROM unioned
