{{config({
    "materialized": "table",
    "unique_key":"event_id",
    "schema":current_date_schema('snowplow')
  })
}}

WITH fishtown AS (
    
    SELECT 
        nullif(jsontext['event_id']::STRING, '') AS event_id
    FROM {{ source('fishtown_snowplow', 'events') }}

), gitlab AS (

    SELECT 
        event_id
    FROM {{ source('gitlab_snowplow', 'events') }}

), unioned AS (

    SELECT event_id 
    FROM fishtown

    UNION ALL

    SELECT event_id
    FROM gitlab

), counts AS (

    SELECT 
        event_id,
        count(event_id) AS event_count
    FROM unioned
    GROUP BY 1
    HAVING event_count > 1

)

SELECT *
FROM counts
