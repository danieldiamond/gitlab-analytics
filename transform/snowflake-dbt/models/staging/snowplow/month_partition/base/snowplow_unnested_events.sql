{{config({
    "unique_key":"event_id"
  })
}}

WITH fishtown as (

    SELECT *
    FROM {{ ref('snowplow_fishtown_unnested_events') }}

), gitlab as (

    SELECT *
    FROM {{ ref('snowplow_gitlab_events') }}

), events_to_ignore as (

    SELECT event_id
    FROM {{ ref('snowplow_duplicate_events') }}

), unioned AS (

    SELECT *
    FROM gitlab

    UNION ALL

    SELECT *
    FROM fishtown

)

SELECT *
FROM unioned
WHERE event_id NOT IN (SELECT event_id FROM events_to_ignore)
