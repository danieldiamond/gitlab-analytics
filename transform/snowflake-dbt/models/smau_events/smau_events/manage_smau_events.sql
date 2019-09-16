WITH manage_snowplow_smau_events AS (
  
  SELECT
    user_snowplow_domain_id,
    user_custom_id::INTEGER   AS gitlab_user_id,
    event_date,
    event_type,
    {{ dbt_utils.surrogate_key('page_view_id', 'event_type') }}
                              AS sk_id,
    'snowplow_frontend'       AS source_type
  
  FROM {{ ref('manage_snowplow_smau_events')}}
  
)

, manage_gitlab_dotcom_smau_events AS (
  
  SELECT
    NULL               AS user_snowplow_domain_id,
    user_id::INTEGER   AS gitlab_user_id,
    event_date,
    event_type,
    sk_id,
    'gitlab_backend'   AS source_type    

  FROM {{ ref('manage_gitlab_dotcom_smau_events')}}
  
)

, unioned AS (
  
  (
    
    SELECT *
    FROM manage_snowplow_smau_events
    
  )
  
  UNION 
  
  (
    
    SELECT *
    FROM manage_gitlab_dotcom_smau_events
    
  )
)

SELECT * 
FROM unioned
