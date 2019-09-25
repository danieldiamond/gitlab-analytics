{{config({
    "materialized":"table",
    "unique_key":"event_id",
    "schema":current_date_schema('snowplow')
  })
}}

WITH fishtown as (

    SELECT *
    FROM {{ ref('snowplow_fishtown_unnested_events') }}

), gitlab as (

    SELECT *
    FROM {{ ref('snowplow_gitlab_events') }}

),

unioned AS (

    SELECT *
    FROM gitlab

    UNION ALL

    SELECT *
    FROM fishtown

), events_to_ignore as (

    SELECT event_id
    FROM unioned
    GROUP BY 1
    HAVING count (*) > 1

)

SELECT *
FROM unioned
WHERE event_id NOT IN (SELECT * FROM events_to_ignore)
