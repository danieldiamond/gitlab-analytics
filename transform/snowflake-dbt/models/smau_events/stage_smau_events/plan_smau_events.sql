WITH plan_snowplow_smau_pageviews_events AS (
  
  SELECT
    user_snowplow_domain_id,
    user_custom_id       AS gitlab_user_id,
    event_date,
    event_type,
    event_surrogate_key  AS event_surrogate_key,
    'snowplow_pageviews' AS source_type
  
  FROM {{ ref('plan_snowplow_smau_pageviews_events')}}
  
)

SELECT * 
FROM plan_snowplow_smau_pageviews_events
