{{
  config({
    "materialized": "incremental"
  })
}}


WITH date_details AS (
  
    SELECT *
    FROM {{ ref('date_details') }}
  
)

, gitlab_dotcom_usage_data_events AS (
  
    SELECT *
    FROM {{ ref('gitlab_dotcom_usage_data_events') }}
  
)

SELECT 
  DATEADD('month', -1, date_day)                  AS smau_month,
  
  -- ids 
  user_id,
  namespace_id,
  
  -- event data
  event_name,
  stage_name,
  
  --metadata
  DATEDIFF('day', user_created_at, date_day)      AS days_since_user_creation,
  DATEDIFF('day', namespace_created_at, date_day) AS days_since_namespace_creation,
  
  COUNT(*)                                        AS event_count,
  COUNT(DISTINCT TO_DATE(event_created_at))       AS event_day_count
FROM date_details
LEFT JOIN gitlab_dotcom_usage_data_events 
  ON gitlab_dotcom_usage_data_events.event_created_at BETWEEN DATEADD('day', -28, date_details.date_day) AND date_day 
WHERE day_of_month = 1
GROUP BY 1,2,3,4,5
