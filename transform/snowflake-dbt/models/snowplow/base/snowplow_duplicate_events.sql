{{config({
    "materialization": "table",
    "unique_key":"event_id",
    "schema":current_date_schema('snowplow')
  })
}}

WITH fishtown AS (
    
    SELECT nullif(jsontext['event_id']::STRING, '') AS event_id
    FROM {{ source('fishtown_snowplow', 'events') }}
    GROUP BY 1
    HAVING count (*) > 1

), gitlab AS (

    SELECT event_id
    FROM {{ source('gitlab_snowplow', 'events') }}
    GROUP BY 1
    HAVING count (*) > 1  

), unioned AS (

    SELECT * 
    FROM fishtown

    UNION

    SELECT *
    FROM gitlab

)

SELECT *
FROM unioned