WITH monitor_snowplow_smau_pageviews_events AS (
  
  SELECT
    user_snowplow_domain_id,
    user_custom_id      AS gitlab_user_id,
    event_date,
    event_type,
    event_surrogate_key AS event_surrogate_key,
    'snowplow_frontend' AS source_type
  
  FROM {{ ref('monitor_snowplow_smau_pageviews_events')}}
  
)

SELECT * 
FROM monitor_snowplow_smau_pageviews_events
