WITH create_snowplow_smau_events AS (
  
  SELECT
    user_snowplow_domain_id,
    user_custom_id      AS gitlab_user_id,
    event_date,
    event_type,
    page_view_id        AS sk_id,
    'snowplow_frontend' AS source_type
  
  FROM {{ ref('create_snowplow_smau_events')}}
  
)

, create_gitlab_dotcom_smau_events AS (
  
  SELECT
    NULL             AS user_snowplow_domain_id,
    user_id          AS gitlab_user_id,
    event_date,
    event_type,
    sk_id,
    'gitlab_backend' AS source_type    

  FROM {{ ref('create_gitlab_dotcom_smau_events')}}
  
)

, unioned AS (
  
  (
    
    SELECT *
    FROM create_snowplow_smau_events
    
  )
  
  UNION 
  
  (
    
    SELECT *
    FROM create_gitlab_dotcom_smau_events
    
  )
)

SELECT * 
FROM unioned