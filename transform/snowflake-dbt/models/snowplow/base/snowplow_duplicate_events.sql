{{config({
    "materialized": "table",
    "unique_key":"event_id",
    "schema":current_date_schema('snowplow')
  })
}}

WITH fishtown AS (
    
    SELECT 
        nullif(jsontext['event_id']::STRING, '') AS event_id,
        count(event_id)                          AS event_count
    FROM {{ source('fishtown_snowplow', 'events') }}
    GROUP BY 1
    HAVING event_count > 1

), gitlab AS (

    SELECT 
        event_id,
        count(event_id)  AS event_count
    FROM {{ source('gitlab_snowplow', 'events') }}
    GROUP BY 1
    HAVING event_count > 1  

), unioned AS (

    SELECT event_id 
    FROM fishtown

    UNION

    SELECT event_id
    FROM gitlab

)

SELECT *
FROM unioned