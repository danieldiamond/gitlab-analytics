WITH create_snowplow_smau_pageviews_events AS (
  
  SELECT
    user_snowplow_domain_id,
    user_custom_id      AS gitlab_user_id,
    event_date,
    event_type,
    event_surrogate_key AS event_surrogate_key,
    'snowplow_pageviews' AS source_type
  
  FROM {{ ref('create_snowplow_smau_pageviews_events')}}
  
)

, create_gitlab_dotcom_smau_events AS (
  
  SELECT
    NULL             AS user_snowplow_domain_id,
    user_id          AS gitlab_user_id,
    event_date,
    event_type,
    event_surrogate_key,
    'gitlab_backend' AS source_type    

  FROM {{ ref('create_gitlab_dotcom_smau_events')}}
  
)

, unioned AS (
  
    SELECT *
    FROM create_snowplow_smau_pageviews_events

    UNION 
    
    SELECT *
    FROM create_gitlab_dotcom_smau_events

)

SELECT * 
FROM unioned
