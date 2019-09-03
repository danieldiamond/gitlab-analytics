WITH good_events AS (

    SELECT *
    FROM {{ ref('snowplow_unnested_events') }}

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
