{{ config({
    "schema": "analytics",
    "post-hook": "grant select on {{this}} to role reporter"
    })
}}


WITH good_events AS (

    SELECT *
    FROM {{ ref('snowplow_unnested_events') }}
    WHERE event_id not in (
        'd1b9015b-f738-4ae7-a4da-a46523a98f15',
        '8de7b076-120b-42b7-922a-d07faded8c8c',
        '1f820848-2b49-4c01-a721-c9d2a2be77a2',
        '246b20a5-b780-4609-b717-b6f3be18c638'
        )

),

bad_events AS (

    SELECT *
    FROM {{ ref('snowplow_unnested_errors') }}

),

good_count AS (

    SELECT
        date_trunc('day',derived_tstamp ::timestamp)::date  AS event_day,
        count(*)                                            AS good_event_count
    FROM good_events
    GROUP BY 1

),

bad_count AS (

    SELECT
        date_trunc('day',failure_timestamp ::timestamp)::date   AS event_day,
        count(*)                                                AS bad_event_count
    FROM bad_events
    GROUP BY 1

)

SELECT
    good_count.event_day,
    good_count.good_event_count,
    bad_count.bad_event_count
FROM good_count
LEFT JOIN bad_count on good_count.event_day = bad_count.event_day
ORDER BY event_day