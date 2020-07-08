{{config({
    "materialized": "view",
    "unique_key":"event_id",
    "schema":current_date_schema('snowplow')
  })
}}

WITH source AS (
    
    SELECT *
    FROM ref("snowplow_duplicate_events_source")

)

SELECT *
FROM source
