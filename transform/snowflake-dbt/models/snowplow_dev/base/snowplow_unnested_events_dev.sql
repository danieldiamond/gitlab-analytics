{% set year_value = var('year', run_started_at.strftime('%Y')) %}
{% set month_value = var('month', run_started_at.strftime('%m')) %}

{{config({
    "materialized":"incremental",
    "unique_key":"base64_event",
    "schema":"snowplow_" + year_value|string + '_' + month_value|string, 
  })
}}

WITH fishtown as (

    SELECT *
    FROM {{ ref('snowplow_fishtown_unnested_events_dev') }}
    {% if is_incremental() %}
       WHERE uploaded_at > (SELECT max(uploaded_at) FROM {{ this }})
    {% endif %}

), gitlab as (

    SELECT *
    FROM {{ ref('snowplow_gitlab_events_dev') }}
    {% if is_incremental() %}
        WHERE uploaded_at > (SELECT max(uploaded_at) FROM {{ this }})
    {% endif %}

), unioned AS (

    SELECT *
    FROM gitlab

    UNION ALL

    SELECT *
    FROM fishtown

)

SELECT *
FROM unioned
