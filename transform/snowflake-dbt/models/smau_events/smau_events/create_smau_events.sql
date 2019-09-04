WITH snowplow_create_activation_events AS (
  
  SELECT
    user_snowplow_domain_id,
    user_custom_id      AS gitlab_user_id,
    event_date,
    event_type,
    page_view_id        AS sk_id,
    'snowplow_frontend' AS source_type
  
  FROM {{ ref('snowplow_create_activation_events')}}
  
)

, gitlab_create_activation_events AS (
  
  SELECT
    NULL             AS user_snowplow_domain_id,
    user_id          AS gitlab_user_id,
    event_date,
    event_type,
    sk_id,
    'gitlab_backend' AS source_type    

  FROM {{ ref('gitlab_dotcom_create_activation_events')}}
  
)

, unioned AS (
  
  (
    
    SELECT *
    FROM snowplow_create_activation_events
    
  )
  
  UNION 
  
  (
    
    SELECT *
    FROM gitlab_create_activation_events
    
  )
)

SELECT * 
FROM unioned
