WITH snowplow_create_activation_events AS (
  
  SELECT
    user_snowplow_domain_id,
    user_custom_id      AS gitlab_user_id,
    event_date,
    event_type,
    page_view_id        AS sk_id,
    'snowplow_frontend' AS source_type
  
  FROM {{ ref('snowplow_plan_activation_events')}}
  
)

SELECT * 
FROM snowplow_create_activation_events
