WITH package_snowplow_smau_pageviews_events AS (
  
  SELECT
    user_snowplow_domain_id,
    user_custom_id       AS gitlab_user_id,
    event_date,
    event_type,
    event_surrogate_key  AS event_surrogate_key,
    'snowplow_pageviews' AS source_type
  
  FROM {{ ref('package_snowplow_smau_pageviews_events')}}
  
)

, package_snowplow_smau_structured_events AS (
  
  SELECT
    user_snowplow_domain_id,
    user_custom_id               AS gitlab_user_id,
    event_date,
    event_type,
    event_surrogate_key          AS event_surrogate_key,
    'snowplow_structured_events' AS source_type
  
  FROM {{ ref('package_snowplow_smau_structured_events')}}
  
)

, unioned AS (
  
  SELECT *
  FROM package_snowplow_smau_pageviews_events
  
  UNION
  
  SELECT *
  FROM package_snowplow_smau_structured_events
  
)

SELECT * 
FROM unioned
