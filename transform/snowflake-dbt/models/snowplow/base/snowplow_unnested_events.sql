{{
  config(
    materialized='incremental',
    unique_key='event_id'
  )
}}

WITH gitlab as (

    SELECT *
    FROM {{ ref('snowplow_gitlab_events') }}
    {% if is_incremental() %}
        WHERE uploaded_at > (SELECT max(uploaded_at) FROM {{ this }})
    {% endif %}

),

fishtown as (

    SELECT *
    FROM {{ ref('snowplow_fishtown_unnested_events') }}
    {% if is_incremental() %}
       WHERE uploaded_at > (SELECT max(uploaded_at) FROM {{ this }})
    {% endif %}

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
