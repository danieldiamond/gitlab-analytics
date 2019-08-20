WITH snowplow_create_activation_events AS (
  
  SELECT
    user_snowplow_domain_id,
    user_custom_id AS gitlab_user_id,
    event_date,
    event_type,
    sk_id
  
  FROM {{ ref('snowplow_create_activation_events')}}
  
)

, gitlab_create_activation_events AS (
  
  SELECT
    NULL    AS user_snowplow_domain_id,
    user_id AS gitlab_user_id,
    event_date,
    event_type,
    sk_id    

  FROM {{ ref('gitab_dotcom_create_activation_events')}}
  
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
