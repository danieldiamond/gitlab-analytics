{% set year_value = env_var('CURRENT_YEAR') %}
{% set month_value = env_var('CURRENT_MONTH') %}

{{config({
    "materialized":"incremental",
    "unique_key":"base64_event",
    "schema":"snowplow_" + year_value|string + '_' + month_value|string, 
  })
}}

WITH events as (

    SELECT *
    FROM {{ ref('snowplow_gitlab_bad_events') }}
    {% if is_incremental() %}
        WHERE uploaded_at > (SELECT max(uploaded_at) FROM {{ this }})
    {% endif %}

)

SELECT *
FROM events
